# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.4.10


from yetl import (
  Config, Timeslice, StageType, Read, DeltaLake
)

# COMMAND ----------

process_group = 1
pipeline = "autoloader"


# COMMAND ----------

project = "ad_works_lt"

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
  process_group=process_group
)


# COMMAND ----------

import dlt
from pyspark.sql.functions import *


def create_dlt(
  source: Read,
  destination: DeltaLake
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
