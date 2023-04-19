# Databricks notebook source
# MAGIC %pip install pyaml pydantic dbxconfig==2.0.0

# COMMAND ----------

from dbxconfig import (
  Config, Timeslice, StageType, Read, DeltaLake
)
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
from dbxconfig import DeltaLake
import os

# COMMAND ----------

pattern = "auto_load_schema"
workspace_path = "/Workspace/autobricks"
config_path = "../Config"
if os.path.exists(workspace_path):
  config_path = f"{workspace_path}/Config"

config_path
# COMMAND ----------

timeslice = Timeslice(day="*", month="*", year="*")
config = Config(config_path=config_path, pattern=pattern)
table_mapping = config.get_table_mapping(timeslice=timeslice, stage=StageType.raw, table="customers")

# COMMAND ----------

def load(
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

  print(stream.columns)

  stream_data:StreamingQuery = (stream
    .select(
      "*",
      fn.current_timestamp().alias("_load_date"),
      "_metadata"
    )
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
    "flag",
    "_corrupt_record as data",
    "_load_date",
    "_metadata"
  ]
  # https://docs.databricks.com/delta/delta-change-data-feed.html
  stream_hf:StreamingQuery = (spark.readStream
    .format("delta") 
    .option("readChangeFeed", "true")
    .table(f"`{destination.database}`.`{destination.table}`")
    .where("_change_type = 'insert' and flag  IN ('H','F')")
    .selectExpr(*columns)
    .writeStream
    .options(**options_hf)
    .trigger(availableNow=True)
    .toTable(f"`{destination.database}`.`{table_hf}`")
  )
  
  if await_termination:
    stream_hf.awaitTermination()

# COMMAND ----------

source = table_mapping.source["customer_details_1"]
raw = table_mapping.destination
config.set_checkpoint(source=source, destination=raw)


# COMMAND ----------

from pprint import pprint
pprint(source.dict())
pprint(raw.dict())

# COMMAND ----------

load(source, raw)

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

