# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.0.5

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("table", "customer_details_1")

# COMMAND ----------

from yetl import (
  Config, Timeslice, StageType, Read, DeltaLake #, ValidationThreshold
)
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
import os
from typing import Union, Dict

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_table = dbutils.widgets.get("table")

print(f"""
  param_process_id: {param_process_id}
  param_table: {param_table}
""")


# COMMAND ----------

timeslice = Timeslice(day="*", month="*", year="*")
project = "project_patterns"
pipeline = "autoload_raw_schema"

config = Config(
  project=project, 
  pipeline=pipeline
)

table_mapping = config.get_table_mapping(
  timeslice=timeslice, 
  stage=StageType.raw, 
  table=param_table
)

from pprint import pprint
pprint(table_mapping.dict())

# COMMAND ----------

def load(
  process_id:int,
  source:Read,
  destination:DeltaLake,
  await_termination:bool = True
):

  stream = (spark.readStream
    .schema(source.spark_schema)
    .format(source.format)
    .options(**source.options)
    .load(source.path)
  )

  src_cols = [c for c in stream.columns if not c.startswith("_")]
  sys_cols = [c for c in stream.columns if c.startswith("_")]

  columns = src_cols + sys_cols + [
    "if(_corrupt_record is null, true, false) as _is_valid",
    f"cast({process_id} as long) as _process_id",
    "current_timestamp() as _load_date",
    "_metadata"
  ]
  print("\n".join(columns))

  stream_data:StreamingQuery = (stream
    .selectExpr(*columns)
    .writeStream
    .options(**destination.options)
    .trigger(availableNow=True)
    .toTable(f"`{destination.database}`.`{destination.table}`")
  )

  
  if await_termination:
    stream_data.awaitTermination()


# COMMAND ----------

import hashlib

def hash_value(value:str):

  hash_object = hashlib.sha224(f"{value}".encode('utf-8'))
  hex_dig = hash_object.hexdigest()
  return hex_dig


# COMMAND ----------



def load_hf(
  source:Union[Read, Dict[str, Read]],
  destination:DeltaLake,
  audit_db:str = "_control",
  table_hf:str = "header_footer",
  await_termination:bool = True
):

  if isinstance(source, dict):
    source_chk_name = "|".join(list(source.keys()))
    source_chk_name = hash_value(source_chk_name)[:7]
  elif isinstance(source, Read):
    source_chk_name = f"{source.database}.{source.table}"
  else:
    raise Exception("Source is invalid type")

  checkpoint = f"{source_chk_name}-{destination.database}.{table_hf}"
  checkpoint = f"/mnt/{destination.container}/checkpoint/{checkpoint}"
  options_hf = {
    "checkpointLocation": checkpoint #,
    # "mergeSchema": True
  }
  print(options_hf)

  header_schema = ",".join([
  "flag string",
  "row_count long",
  "period long",
  "batch string"
  ])

  footer_schema = ",".join([
    "flag string",
    "name string",
    "period long"
  ])

  database = f"{destination.database}{audit_db}"
  database_table = f"`{database}`.`{table_hf}`"

  columns = [
    f"from_csv(_corrupt_record, '{header_schema}') as header",
    "_corrupt_record as raw_header",
    "_process_id",
    "_load_date",
    "_metadata"
  ]
  # https://docs.databricks.com/delta/delta-change-data-feed.html
  stream_header:StreamingQuery = (spark.readStream
    .format("delta") 
    .option("readChangeFeed", "true")
    .table(f"`{destination.database}`.`{destination.table}`")
    .where("_change_type = 'insert' and flag = 'H'")
    .selectExpr(*columns)
  )

  columns = [
    f"from_csv(_corrupt_record, '{footer_schema}') as footer",
    "_corrupt_record as raw_footer",
    "_process_id as f_process_id",
    "_metadata as f_metadata"
  ]
  stream_footer:StreamingQuery = (spark.readStream
    .format("delta") 
    .option("readChangeFeed", "true")
    .table(f"`{destination.database}`.`{destination.table}`")
    .where("_change_type = 'insert' and flag = 'F'")
    .selectExpr(*columns)
  )

  columns = [
    "header",
    "raw_header",
    "footer",
    "raw_footer",
    "_process_id",
    "_load_date",
    "_metadata"
  ]
  stream_joined = (stream_header
    .join(
      stream_footer, 
      fn.expr("""
        _process_id = f_process_id AND
        _metadata.file_name = f_metadata.file_name
      """))
    .selectExpr(*columns)
  )

  stream_write = (stream_joined
    .writeStream
    .options(**options_hf)
    .trigger(availableNow=True)
    .toTable(database_table)
  )
  
  if await_termination:
    stream_write.awaitTermination()

# COMMAND ----------

def load_audit(
  process_id:int,
  destination:DeltaLake,
  audit_db:str = "_control",
  table_hf:str = "header_footer",
  table_audit = "etl_audit"
):

  database = f"{destination.database}{audit_db}"
  database_table = f"`{database}`.`{table_audit}`"

  df = spark.sql(f"""
    SELECT
      cast(count(*) as long) as total_count,
      cast(sum(if(d._is_valid, 1, 0)) as long) as valid_count,
      cast(sum(if(d._is_valid, 0, 1)) as long) as invalid_count,
      if(ifnull(cast(count(*) as long), 0)=0, 0.0,
        cast(sum(if(d._is_valid, 0, 1)) as long) / cast(count(*) as long)
      ) as invalid_ratio,
      hf.header.row_count as expected_row_count,
      hf._process_id,
      hf._load_date,
      d._metadata.file_name,
      d._metadata.file_path,
      d._metadata.file_size,
      d._metadata.file_modification_time
    FROM `{destination.database}`.`{destination.table}` as d
    JOIN `{database}`.`{table_hf}` as hf
      ON hf._process_id = d._process_id
      AND hf._metadata.file_name = d._metadata.file_name
    WHERE d._process_id = {process_id}
    GROUP BY 
      hf.header.row_count,
      hf._process_id,
      hf._load_date,
      d._metadata.file_name,
      d._metadata.file_path,
      d._metadata.file_size,
      d._metadata.file_modification_time
  """)

  columns = [
    "total_count",
    "valid_count",
    "invalid_count",
    "invalid_ratio",
    "expected_row_count",
    "_process_id",
    "_load_date",
    "file_name",
    "file_path",
    "file_size",
    "file_modification_time"
  ]
  result = (df.select(*columns).write
    .format("delta")
    .mode("append")
    .saveAsTable(database_table)
  )



# COMMAND ----------

raw = table_mapping.destination
if isinstance(table_mapping.source, dict):

  for _, source in table_mapping.source.items():
    config.set_checkpoint(source, raw)
    print(raw.options["checkpointLocation"])
    load(param_process_id, source, raw)

else:
  config.set_checkpoint(table_mapping.source, raw)
  print(raw.options["checkpointLocation"])
  load(param_process_id, table_mapping.source, raw)

# COMMAND ----------

load_hf(table_mapping.source, raw)


# COMMAND ----------

load_audit(param_process_id, raw)

# COMMAND ----------

def create_threhsholds_views(
  destination:DeltaLake,
  thresholds,
  threshold_type:str,
  audit_db:str = "_control",
  table_audit = "etl_audit"
):
  if thresholds:
    
    database = f"{destination.database}{audit_db}"
    view = f"{threshold_type}_{destination.table}"

    select = []
    where = []
    if thresholds.invalid_ratio is not None:
      select.append(f"{thresholds.invalid_ratio} as threshold_invalid_ratio,")
      where.append(f"invalid_ratio > {thresholds.invalid_ratio}")

    if thresholds.max_rows is not None:
      select.append(f"{thresholds.max_rows} as threshold_min_count,")
      where.append(f"total_count > {thresholds.max_rows}")

    if thresholds.max_rows is not None:
      select.append(f"{thresholds.max_rows} as threshold_max_count,")
      where.append(f"total_count < {thresholds.min_rows}")

    if thresholds.invalid_rows is not None:
      select.append(f"{thresholds.invalid_rows} as threshold_invalid_count,")
      where.append(f"invalid_count > {thresholds.invalid_rows}")

    select = "\\n".join(select)
    where = "OR\\n".join(where)

    sql = f"""
      CREATE OR REPLACE VIEW `{database}`.`{view}`
      AS 
      select
        invalid_ratio,
        total_count,
        expected_row_count,
        valid_count,
        invalid_count,
        {select}
        file_name,
        _process_id
      from `{database}`.`{table_audit}`
      where _process_id = {param_process_id}
      and (
        {where} OR
        expected_row_count != valid_count
      );
    """
    print(sql)
    spark.sql(sql)
    df = spark.sql(f"select * from `{database}`.`{view}`")

    return df

# COMMAND ----------

if raw.exception_thresholds:
  df = create_threhsholds_views(raw, raw.exception_thresholds, "threshold_exception")
  display(df)
  if df.count() > 0:
    msg = f"Exception thresholds have been exceeded {table_mapping.destination.database}.{table_mapping.destination.table}"
    raise Exception(msg)

# COMMAND ----------

if raw.warning_thresholds:
  df = create_threhsholds_views(raw, raw.warning_thresholds, "threshold_warning")
  display(df)
  # if df.count() > 0:
  #   msg = f"Warning: thresholds have been exceeded {table_mapping.destination.database}.{table_mapping.destination.table}"
  #   print(msg)

# COMMAND ----------

msg = f"Succeeded: {raw.database}.{raw.table}"
dbutils.notebook.exit(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from raw_dbx_patterns.customers
# MAGIC -- select * from raw_dbx_patterns.customer_details_1
# MAGIC select * from raw_dbx_patterns.customer_details_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), `_metadata`.file_name  from raw_dbx_patterns.customers group by `_metadata`.file_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns_control.header_footer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns_control.etl_audit
