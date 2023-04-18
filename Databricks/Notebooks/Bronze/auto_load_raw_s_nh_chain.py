# Databricks notebook source
# MAGIC %pip install pyaml pydantic dbxconfig==1.0.5

# COMMAND ----------

import os

os.chdir("/Workspace/autobricks/")
os.getcwd()

# COMMAND ----------

from dbxconfig import Config, Timeslice, StageType
import json

pattern = "auto_load_schema"
config_path = f"../Config/{pattern}.yaml"
timeslice = Timeslice(day="*", month="*", year="*")
config = Config(config_path=config_path)
table_mapping = config.get_table_mapping(timeslice=timeslice, stage=StageType.raw, table="customers")

table_mapping.destination


# COMMAND ----------

from pyspark.sql import functions as fn
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery

def load(
  source:str,
  destination:str,
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
      "_metadata.*"
    )
    .writeStream
    .options(**destination.options)
    .trigger(availableNow=True)
    .toTable(f"`{destination.database}`.`{destination.table}`")
  )

  
  if await_termination:
    stream_data.awaitTermination()


# COMMAND ----------

from pyspark.sql import functions as fn
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery

def load_hf(
  source:str,
  destination:str,
  await_termination:bool = True
):

  table_hf = "headerfooter"
  checkpoint = f"{source.database}.{source.table}-{destination.database}.{table_hf}"
  options_hf = {
    "checkpointLocation": f"/mnt/{destination.container}/checkpoint/{checkpoint}"
  }

  # https://docs.databricks.com/delta/delta-change-data-feed.html
  stream_hf:StreamingQuery = (spark.readStream
    .format("delta") 
    .option("readChangeFeed", "true")
    .table(f"`{destination.database}`.`{destination.table}`")
    .where("_change_type = 'insert' and flag  IN ('H','F')")
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
config.link_checkpoint(source=source, destination=raw)


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

dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns/customers", True)
dbutils.fs.rm("/mnt/datalake/checkpoint", True)
spark.sql("drop database if exists raw_dbx_patterns CASCADE")
spark.sql("drop database if exists base_dbx_patterns CASCADE")

