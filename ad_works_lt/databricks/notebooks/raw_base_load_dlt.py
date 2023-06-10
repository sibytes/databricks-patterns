# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.4.14

# COMMAND ----------


from yetl import (
  Config, StageType, Read, DeltaLake
)
from yetl.workflow import create_dlt
from pyspark.sql.functions import *


# COMMAND ----------

process_group = 1
pipeline = "batch"
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

    return df

# COMMAND ----------


def create_base_dlt(
  source: Read,
  destination: DeltaLake
):

  @dlt.table(
    name=f"base_{destination.table}"
  )
  def base_load():
    return(
      dlt.read(f"raw_{source.table}").where("_is_valid = 1")
    )



# COMMAND ----------

create_dlt(config, StageType.raw, create_raw_dlt, process_group=process_group)

# COMMAND ----------

create_dlt(config, StageType.base, create_raw_dlt, process_group=process_group)

# COMMAND ----------

dbutils.notebook.exit("Success")
