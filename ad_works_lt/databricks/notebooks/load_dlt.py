# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.6.6.dev1

# COMMAND ----------


from yetl import (
  Config, StageType, Read, DeltaLake
)
from yetl.workflow import create_dlt
from pyspark.sql.functions import *


# COMMAND ----------

process_group = 1
pipeline = "autoloader_dlt"
project = "ad_works_lt"
config = Config(
  project=project, 
  pipeline=pipeline
)

# COMMAND ----------


import dlt
def create_raw_dlt(
  source: Read,
  destination: DeltaLake
):

  @dlt.table(
    name=f"raw_{destination.table}"
  )
  def raw_load():

    df = (
        spark.readStream.schema(source.spark_schema)
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
            "current_timestamp() as _load_date",
            "_metadata",
        ]
    )

    df = df.selectExpr(*columns)
    df = source.add_timeslice(df)

    return df

# COMMAND ----------

create_dlt(
  config=config, 
  stage=StageType.raw, 
  dlt_funct=create_raw_dlt, 
  process_group=process_group
)

# COMMAND ----------

from yetl.workflow import create_dlt
from yetl import (
  Config, StageType
)

process_group = 1
pipeline = "batch"
project = "ad_works_lt"
config = Config(
  project=project, 
  pipeline=pipeline
)

# COMMAND ----------


def create_base_dlt(
  source: Read,
  destination: DeltaLake
):

  @dlt.table(
    name=destination.table
  )
  def base_load():
    return(
      dlt.read(f"raw_{source.table}").where("_is_valid = 1")
    )



# COMMAND ----------

create_dlt(
  config=config, 
  stage=StageType.base, 
  dlt_funct=create_base_dlt, 
  process_group=process_group
)

# COMMAND ----------

dbutils.notebook.exit("Success")
