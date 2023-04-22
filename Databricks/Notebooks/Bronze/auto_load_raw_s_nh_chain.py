# Databricks notebook source
# MAGIC %pip install pyaml pydantic dbxconfig==2.0.0

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

pattern = "auto_load_schema"
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
  source:Read,
  destination:DeltaLake,
  await_termination:bool = True
):

  table_hf = "headerfooter"
  checkpoint = f"{source.database}.{source.table}-{destination.database}.{table_hf}"
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

  location = destination.location.replace(destination.table, table_hf)
  spark.sql(f"""
  CREATE TABLE IF NOT EXISTS `{destination.database}`.`{table_hf}`
  USING DELTA
  LOCATION '{location}'
  TBLPROPERTIES (
    delta.appendOnly = true,
    delta.autoOptimize.autoCompact = true,
    delta.autoOptimize.optimizeWrite = true
  )
  """)

  columns = [
    f"to_json(_corrupt_record, '{header_schema}') as header",
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
    f"to_json(_corrupt_record, '{footer_schema}') as footer",
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
    .toTable(f"`{destination.database}`.`{table_hf}`")
  )
  
  if await_termination:
    stream_write.awaitTermination()

# COMMAND ----------

def load_audit(
  source:Read,
  destination:DeltaLake,
  await_termination:bool = True
):

  table_audit = "etl_audit"
  location = destination.location.replace(destination.table, table_audit)
  spark.sql(f"""
  CREATE TABLE IF NOT EXISTS `{destination.database}`.`{table_audit}`
  USING DELTA
  LOCATION '{location}'
  TBLPROPERTIES (
    delta.appendOnly = true,
    delta.autoOptimize.autoCompact = true,
    delta.autoOptimize.optimizeWrite = true
  )
  """)

  to_table = f"`{destination.database}`.`headerfooter`"
  df = spark.sql(f"""
    SELECT
      cast(count(*) as long) as total_count,
      cast(sum(if(d._is_valid, 1, 0)) as long) as valid_count,
      cast(sum(if(d._is_valid, 0, 1)) as long) as invalid_count,
      hf.header.row_count as expected_row_count,
      d._metadata.*
    FROM `{destination.database}`.`{destination.table}` as d
    JOIN {to_table} as hf
      ON hf._process_id = d._process_id
      AND hf._metadata.file_name = d._metadata.file_name
    GROUP BY 
      hf.header.row_count, 
      d._metadata.*
  """)

  result = (df.write
    .format("delta")
    .mode("append")
    .option("schemaMerge", True)
    .saveAsTable(to_table)
  )

# COMMAND ----------

raw = table_mapping.destination

if isinstance(table_mapping.source, dict):
  for _, source in table_mapping.source.items():
    print(f"""setting checkpoint
      load: {source.database}.{source.database} to {raw.database}.{raw.database}
      path: {raw.checkpoint}
    """)
    config.set_checkpoint(source=source, destination=raw)
else:
  source = table_mapping.source
  print(f"""setting checkpoint
    load: {source.database}.{source.database} to {raw.database}.{raw.database}
    path: {raw.checkpoint}
  """)
  config.set_checkpoint(source=source, destination=raw)


# COMMAND ----------

from pprint import pprint
pprint(source.dict())
pprint(raw.dict())

# COMMAND ----------

load(param_process_id, source, raw)

# COMMAND ----------

load_hf(source, raw)


# COMMAND ----------

dbutils.notebook.exit("Succeeded")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns.headerfooter

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns", True)
dbutils.fs.rm("/mnt/datalake/checkpoint", True)
spark.sql("drop database if exists raw_dbx_patterns CASCADE")
spark.sql("drop database if exists base_dbx_patterns CASCADE")

