# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==3.0.0.dev3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("table", "raw_balance")
dbutils.widgets.text("catalog", "development")

# COMMAND ----------

from yetl import (
  Config, Timeslice, StageType, DeltaLake, Read, DeltaLake
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame
from typing import Union
from pyspark.sql import functions as fn
import logging

# COMMAND ----------

def read_data(reader, source:Read):

    return (
        reader.schema(source.spark_schema)
        .format(source.format)
        .options(**source.options)
        .load(source.path)
    )

def get_metadata_columns(source:Read, process_id:int):
  return [
          f"cast(null as timestamp) as {source.slice_date_column_name}",
          f"cast({process_id} as long) as _process_id",
          "current_timestamp() as _load_date",
          "_metadata",
        ]

def transform(df:DataFrame, source:Read, process_id:int):

  src_cols = [c for c in df.columns if not c.startswith("_")]
  sys_cols = [c for c in df.columns if c.startswith("_")]

  columns = (
      src_cols
      + sys_cols
      + get_metadata_columns(source, process_id)
  )

  df = df.selectExpr(*columns)
  df = source.add_timeslice(df)

  return df

def load(
    process_id: int,
    source: Read,
    destination: DeltaLake,
    drop_already_loaded:bool = False
):
    df = read_data(spark.readStream, source)

    df = transform(df, source, process_id)


    stream_data: StreamingQuery = (df
        .select("*")
        .writeStream
        .options(**destination.options)
        .trigger(availableNow=True)
        .toTable(destination.qualified_table_name())
    )

    stream_data.awaitTermination()



# COMMAND ----------

# DBTITLE 1,Handle & Validate Parameters
param_process_id = int(dbutils.widgets.get("process_id"))
param_table = dbutils.widgets.get("table")
param_load_type = "autoloader"
param_timeslice = "*"
param_catalog = dbutils.widgets.get("catalog")
project = "ooo_loading"

timeslice = Timeslice.parse_iso_date(param_timeslice)

print(f"""
  project : {project}
  param_process_id: {param_process_id}
  param_table: {param_table}
  load_type: {param_load_type}
  timeslice: {timeslice}
  catalog: {param_catalog}
""")

# COMMAND ----------

pipeline = param_load_type
config = Config(
  project=project, 
  pipeline=pipeline,
  timeslice=timeslice
)

# COMMAND ----------

# Load the data

table_mapping = config.get_table_mapping(
  stage=StageType.raw, 
  table=param_table,
  create_table=False,
  catalog=param_catalog
)
config.set_checkpoint(
  table_mapping.source, table_mapping.destination
)



# COMMAND ----------

table_mapping.destination.options

# COMMAND ----------


load(
  param_process_id, table_mapping.source, table_mapping.destination
)


# COMMAND ----------

msg = f"Succeeded: {table_mapping.destination.qualified_table_name()}"
dbutils.notebook.exit(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select id, code_1, code_2, code_3, code_4, code_5, count(1)
# MAGIC from `development`.`ooo_loading`.`raw_balance`
# MAGIC group by all
# MAGIC having count(1) > 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   transacted, 
# MAGIC   created, 
# MAGIC   amount, 
# MAGIC   qauntity, 
# MAGIC   price, 
# MAGIC   tax, 
# MAGIC   value
# MAGIC from `development`.`ooo_loading`.`raw_balance`
# MAGIC where 1=1
# MAGIC and `id` = 13
# MAGIC and `code_1` = '656684944-0'
# MAGIC and `code_2` = '267161185-4' 
# MAGIC and `code_3` = '036389464-0'
# MAGIC and `code_4` = '023012256-6'
# MAGIC and `code_5` = '989988643-2'
# MAGIC order by created

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from `development`.`ooo_loading`.`raw_balance`
