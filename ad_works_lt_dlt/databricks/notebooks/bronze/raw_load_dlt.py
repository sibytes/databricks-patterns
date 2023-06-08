# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.4.10

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("timeout", "3600")
dbutils.widgets.text("process_group", "1")
dbutils.widgets.text("load_type", "batch")
dbutils.widgets.text("timeslice", "*")
dbutils.widgets.text("drop_already_loaded", "True")

# COMMAND ----------


from yetl import (
  Config, Timeslice, StageType, Read, DeltaLake
)
from yetl.workflow import (
  execute_notebooks, Notebook
)

from enum import Enum

class LoadType(Enum):
    autoloader = "autoloader"
    batch = "batch"

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_timeout = int(dbutils.widgets.get("timeout"))
param_process_group = int(dbutils.widgets.get("process_group"))
param_load_type = dbutils.widgets.get("load_type")
param_timeslice = dbutils.widgets.get("timeslice")
param_drop_already_loaded = dbutils.widgets.get("drop_already_loaded")

if param_drop_already_loaded.lower() in ['true','false']:
  param_drop_already_loaded = bool(param_drop_already_loaded)
else:
  raise ValueError("drop_already_loaded must be true or false")

try:
  load_type:LoadType = LoadType(param_load_type)
except Exception as e:
   raise Exception(f"load_type parameter {param_load_type} is not valid")

print(f"""
  param_process_id: {param_process_id}
  param_timeout: {param_timeout}
  process_group: {param_process_group}
  load_type: {str(load_type)}
  timeslice: {param_timeslice}
""")

# COMMAND ----------

project = "ad_works_lt_dlt"
pipeline = load_type.value

config = Config(
  project=project, 
  pipeline=pipeline
)

# COMMAND ----------

tables = config.tables.lookup_table(
  stage=StageType.raw, 
  first_match=False,
  # this will filter the tables on a custom property
  # in the tables parameter you can add whatever custom properties you want
  # either for filtering or to use in pipelines
  process_group=param_process_group
)


# COMMAND ----------

import dlt
from pyspark.sql.functions import *


def create_dlt(
  source: Read,
  destination: DeltaLake,
  drop_already_loaded:bool = True
):

  @dlt.table(
    name=destination.table
  )
  def raw_load():

    df:DataFrame = (
        spark.read.schema(source.spark_schema)
        .format(source.format)
        .options(**source.options)
        .load(source.path)
    )

    src_cols = [c for c in df.columns if not c.startswith("_")]
    sys_cols = [c for c in df.columns if c.startswith("_")]

    columns = (
        src_cols
        + sys_cols
        + [
            "if(_corrupt_record is null, true, false) as _is_valid",
            f"cast(null as timestamp) as {source.slice_date_column_name}",
            # f"cast({process_id} as long) as _process_id",
            "current_timestamp() as _load_date",
            "_metadata",
        ]
    )

    df = df.selectExpr(*columns)
    df = source.add_timeslice(df)

    if drop_already_loaded:
      drop_if_already_loaded(df, source)

    return df



# COMMAND ----------

for t in tables:

  table_mapping = config.get_table_mapping(
    stage=StageType.raw, 
    table=t.table,
    create_table=False
  )
  config.set_checkpoint(
    table_mapping.source, table_mapping.destination
  )

  create_dlt(
    table_mapping.source, 
    table_mapping.destination, 
    False
  )


# COMMAND ----------

dbutils.notebook.exit("Success")
