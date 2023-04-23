# Databricks notebook source
# MAGIC %pip install pyaml pydantic dbxconfig==2.2.0

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("table", "customers")

# COMMAND ----------

from dbxconfig import (
  Config, Timeslice, StageType, Read, DeltaLake
)
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
from dbxconfig import DeltaLake
import os

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_table = dbutils.widgets.get("table")
print(f"""
  param_process_id: {param_process_id}
  param_table: {param_table}
""")

# COMMAND ----------

def clear_down():
  dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns", True)
  dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns_control", True)
  dbutils.fs.rm("/mnt/datalake/checkpoint", True)
  spark.sql("drop database if exists raw_dbx_patterns CASCADE")
  spark.sql("drop database if exists raw_dbx_patterns_control CASCADE")
clear_down()

# COMMAND ----------

pattern = "autoload_raw_schema"
workspace_path = "/Workspace/autobricks"
repo_path = "/Workspace/Repos"
config_path = "../Config"
if os.path.exists(workspace_path) and not os.getcwd().startswith(repo_path):
  config_path = f"{workspace_path}/Config"

config_path

# COMMAND ----------

timeslice = Timeslice(day="*", month="*", year="*")
config = Config(config_path=config_path, pattern=pattern)
table_mapping = config.get_table_mapping(timeslice=timeslice, stage=StageType.raw, table=param_table)

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

def load_hf(
  destination:DeltaLake,
  audit_db:str = "_control",
  table_hf:str = "header_footer",
  await_termination:bool = True
):

  checkpoint = f"{destination.database}.{table_hf}"
  options_hf = {
    "checkpointLocation": f"/mnt/{destination.container}/checkpoint/{checkpoint}",
    "mergeSchema": True
  }

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
  location = destination.location.replace(destination.table, table_hf)
  location = location.replace(destination.database, database)
  sql_db = f"CREATE DATABASE IF NOT EXISTS `{database}`"
  sql_table = f"""
  CREATE TABLE IF NOT EXISTS {database_table}
  USING DELTA
  LOCATION '{location}'
  TBLPROPERTIES (
    delta.appendOnly = true,
    delta.autoOptimize.autoCompact = true,
    delta.autoOptimize.optimizeWrite = true
  )
  """
  print(sql_table)
  spark.sql(sql_db)
  spark.sql(sql_table)

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
    "raw_footer",
    "footer",
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
  source:Read,
  destination:DeltaLake,
  audit_db:str = "_control",
  table_hf:str = "header_footer",
  table_audit = "etl_audit"
):

  database = f"{destination.database}{audit_db}"
  database_table = f"`{database}`.`{table_audit}`"
  location = destination.location.replace(destination.table, table_audit)
  location = location.replace(destination.database, database)

  sql_db = f"CREATE DATABASE IF NOT EXISTS `{database}`"
  sql_table = f"""
  CREATE TABLE IF NOT EXISTS {database_table}
  USING DELTA
  LOCATION '{location}'
  TBLPROPERTIES (
    delta.appendOnly = true,
    delta.autoOptimize.autoCompact = true,
    delta.autoOptimize.optimizeWrite = true
  )
  """
  print(sql_table)
  spark.sql(sql_db)
  spark.sql(sql_table)

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


  result = (df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(database_table)
  )



# COMMAND ----------

from pprint import pprint
raw = table_mapping.destination
source = table_mapping.source["customer_details_1"]

pprint(source.dict())
pprint(raw.dict())

# COMMAND ----------

for _, source in table_mapping.source.items():
  config.set_checkpoint(source, raw)
  print(raw.options["checkpointLocation"])
  load(param_process_id, source, raw)

# COMMAND ----------

load_hf(raw)


# COMMAND ----------

load_audit(param_process_id, source, raw)

# COMMAND ----------

def apply_threhsholds(
  thresholds
):
  if thresholds:
    
    invalid_ratio_select = ""
    invalid_ratio_where = ""
    max_rows_select = "" 
    max_rows_where = "" 
    min_rows_select = "" 
    min_rows_where = "" 
    invalid_rows_select = ""
    invalid_rows_where = ""

    if thresholds.invalid_ratio is not None:
      invalid_ratio_select = f"{thresholds.invalid_ratio} as threshold_invalid_ratio,"
      invalid_ratio_where = f"invalid_ratio > {thresholds.invalid_ratio} OR"

    if thresholds.max_rows is not None:
        max_rows_select = f"{thresholds.max_rows} as threshold_min_count,"
        max_rows_where = f"total_count > {thresholds.max_rows} OR"

    if thresholds.max_rows is not None:
        min_rows_select = f"{thresholds.max_rows} as threshold_max_count,"
        min_rows_where = f"total_count < {thresholds.min_rows} OR"

    if thresholds.invalid_rows is not None:
        invalid_rows_select = f"{thresholds.invalid_rows} as threshold_invalid_count,"
        invalid_rows_where = f"invalid_count > {thresholds.invalid_rows} OR"

    sql = f"""
      select
        invalid_ratio,
        total_count,
        expected_row_count,
        valid_count,
        invalid_count,
        {invalid_ratio_select}
        {min_rows_select}
        {max_rows_select}
        {invalid_rows_select}
        file_name,
        _process_id
      from raw_dbx_patterns_control.etl_audit
      where _process_id = {param_process_id}
      and (
        {invalid_ratio_where}
        {invalid_rows_where}
        {min_rows_where}
        {max_rows_where}
        expected_row_count != valid_count
      )
    """
    df = spark.sql(sql)
    return df

# COMMAND ----------

if raw.exception_thresholds:
  df = apply_threhsholds(raw.exception_thresholds)
  display(df)
  if df.count() > 0:
    msg = f"Exception thresholds have been exceeded {table_mapping.destination.database}.{table_mapping.destination.table}"
    raise Exception(msg)

# COMMAND ----------

if raw.warning_thresholds:
  df = apply_threhsholds(raw.warning_thresholds)
  display(df)
  if df.count() > 0:
    msg = f"Warning: thresholds have been exceeded {table_mapping.destination.database}.{table_mapping.destination.table}"
    print(msg)

# COMMAND ----------

dbutils.notebook.exit("Succeeded")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns.customers 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), `_metadata`.file_name  from raw_dbx_patterns.customers group by `_metadata`.file_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns_control.header_footer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns_control.etl_audit
