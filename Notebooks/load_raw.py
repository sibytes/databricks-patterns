# Databricks notebook source
# MAGIC %pip install pyaml pydantic

# COMMAND ----------

from common import Config, Timeslice
timeslice = Timeslice(day=1, month=1, year=2023)
config = Config(timeslice=timeslice)
table_stages = config.tables[1]


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

  stream:StreamingQuery = source.rename_headerless(stream)
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

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns.headerfooter

# COMMAND ----------

dbutils.notebook.exit("Succeeded")

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns/customers", True)
dbutils.fs.rm("/mnt/datalake/checkpoint/raw_dbx_patterns_customers", True)
dbutils.fs.rm("/mnt/datalake/checkpoint/raw_dbx_patterns_headerfooter", True)
spark.sql("drop database if exists raw_dbx_patterns CASCADE")
spark.sql("drop database if exists base_dbx_patterns CASCADE")

# COMMAND ----------

dbutils.fs.ls("/mnt/lake/checkpoint")

# COMMAND ----------

options = {
  'delimiter': ',',
  'emptyValue': '',
  'inferSchema': False,
  'encoding': 'windows-1252',
  'escape': '"',
  'header': False,
  'mode': 'PERMISSIVE',
  'nullValue': '',
  'quote': '"'
}
from common._utils import load_schema
schema = load_schema('../Schema/balance.json')
schema.add(field="_corrupt_record", data_type="string")
df = spark.read.format("csv").schema(schema).options(**options).load("/mnt/landing/FNZ/default/Balance/2023/01/01/FNZDataAPI-balance-20230101*.dat")

display(df.where("LOAD_FLAG in ('H','F')"))

# COMMAND ----------

display(df.where("LOAD_FLAG not in ('H','F')"))
