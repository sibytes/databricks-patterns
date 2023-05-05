# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.0.1

# COMMAND ----------

from common import Config, Timeslice
pattern = "auto_load_schema_evolution"
config_path = f"../Config/{pattern}.yaml"
timeslice = Timeslice(day="*", month="*", year="*")
config = Config(timeslice=timeslice, config_path=config_path)
table_stages = config.tables[0]


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
    .format(source.format)
    .options(**source.options)
    .load(source.path)
  )

  print(stream.columns)

  stream_data:StreamingQuery = (stream
    .where("flag  NOT IN ('H','F')")
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

  table_hf = "headerfooter"
  checkpoint = f"{source.database}.{source.table}-{destination.database}.{table_hf}"
  options_hf = {
    "checkpointLocation": f"/mnt/{destination.container}/checkpoint/{checkpoint}"
  }

  stream_hf:StreamingQuery = (stream
    .where("flag  IN ('H','F')")
    .select(
      "*",
      fn.current_timestamp().alias("_load_date"),
      "_metadata.*"
    )
    .writeStream
    .options(**options_hf)
    .trigger(availableNow=True)
    .toTable(f"`{destination.database}`.`{table_hf}`")
  )
  
  if await_termination:
    stream_data.awaitTermination()
    stream_hf.awaitTermination()



# COMMAND ----------

source = config.get_source(table=table_stages.source)
raw = config.get_raw(table=table_stages.raw)
config.link_checkpoint(source, raw)


# COMMAND ----------

from pprint import pprint
pprint(source.dict())
pprint(raw.dict())

# COMMAND ----------

load(source, raw)

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
