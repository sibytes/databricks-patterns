# Databricks notebook source
# MAGIC %pip install pyaml pydantic

# COMMAND ----------

from common import Config, Timeslice
timeslice = Timeslice(day=1, month=1, year=2023)
config = Config(timeslice=timeslice)
table_stages = config.tables[0]
source = config.get_source(table=table_stages.source)
source.dict()

# COMMAND ----------

raw = config.get_raw(source_table=table_stages.source, table=table_stages.raw)
raw.dict()

# COMMAND ----------

base = config.get_raw(source_table=table_stages.raw, table=table_stages.base)
base.dict()
