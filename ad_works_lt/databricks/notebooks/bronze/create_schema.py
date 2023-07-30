# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==2.0.4.dev1

# COMMAND ----------

dbutils.widgets.text("load_type", "batch")

# COMMAND ----------

from etl import LoadType
from yetl import (
  Config, StageType, Read, DeltaLake, Timeslice
)

# COMMAND ----------

param_load_type = dbutils.widgets.get("load_type")

try:
  load_type:LoadType = LoadType(param_load_type)
except Exception as e:
   raise Exception(f"load_type parameter {param_load_type} is not valid")

print(f"""
  load_type: {str(load_type)}
""")

# COMMAND ----------

import yaml, os

def create_schema(
  source:Read
):

  options = source.options
  options["inferSchema"] = True
  options["enforceSchema"] = False

  df = (
    spark.read
    .format(source.format)
    .options(**options)
    .load(source.path)
  )

  schema = yaml.safe_load(df.schema.json())
  schema = yaml.safe_dump(schema, indent=4)

  with open(source.spark_schema, "w", encoding="utf-8") as f:
    f.write(schema)


# COMMAND ----------

project = "ad_works_lt"
pipeline = load_type.value

config = Config(
  project=project, 
  pipeline=pipeline,
  timeslice=Timeslice(year=2023, month=1, day=1)
)

tables = config.lookup_table(
  stage=StageType.raw, 
  first_match=False
)

for t in tables:
  table_mapping = config.get_table_mapping(
    t.stage, 
    t.table, 
    t.database, 
    create_table=False)
  create_schema(table_mapping.source)


