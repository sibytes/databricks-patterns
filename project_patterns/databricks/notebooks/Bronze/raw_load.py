# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.0.2

# COMMAND ----------

def clear_down():
  dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns", True)
  dbutils.fs.rm("/mnt/datalake/data/raw/raw_dbx_patterns_control", True)
  dbutils.fs.rm("/mnt/datalake/checkpoint", True)
  spark.sql("drop database if exists raw_dbx_patterns CASCADE")
  spark.sql("drop database if exists raw_dbx_patterns_control CASCADE")
clear_down()

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("max_parallel", "4")
dbutils.widgets.text("timeout", "3600")
dbutils.widgets.text("process_group", "1")

# COMMAND ----------

from yetl import (
  Config, Timeslice, StageType, Tables
)
from yetl.workflow import (
  execute_notebooks, Notebook
)
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
from yetl import DeltaLake
import os

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_max_parallel = int(dbutils.widgets.get("max_parallel"))
param_timeout = int(dbutils.widgets.get("timeout"))
param_process_group= int(dbutils.widgets.get("process_group"))
print(f"""
  param_process_id: {param_process_id}
  param_max_parallel: {param_max_parallel}
  param_timeout: {param_timeout}
  process_group: {param_process_group}
""")

# COMMAND ----------

project = "project_patterns"
pipeline = "autoload_raw_schema"

config = Config(
  project=project, 
  pipeline=pipeline
)
tables = config.tables.lookup_table(
  stage=StageType.raw, 
  first_match=False,
  process_group=param_process_group
)
msg_tables = '\n'.join([f"{t.database}.{t.table}" for t in tables])
print(f"{msg_tables}")


# COMMAND ----------

# MAGIC   %sql
# MAGIC   CREATE DATABASE IF NOT EXISTS `raw_dbx_patterns_control`;
# MAGIC   CREATE TABLE IF NOT EXISTS `raw_dbx_patterns_control`.`header_footer`
# MAGIC   (
# MAGIC     header struct<flag:string,row_count:bigint,period:bigint,batch:string>,
# MAGIC     raw_header string,
# MAGIC     footer struct<flag:string,name:string,period:bigint>,
# MAGIC     raw_footer string,
# MAGIC     _process_id bigint,
# MAGIC     _load_date timestamp,
# MAGIC     _metadata struct<file_path:string,file_name:string,file_size:bigint,file_modification_time:timestamp>
# MAGIC   )
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/datalake/data/raw/raw_dbx_patterns_control/header_footer'
# MAGIC   TBLPROPERTIES (
# MAGIC     delta.appendOnly = true,
# MAGIC     delta.autoOptimize.autoCompact = true,
# MAGIC     delta.autoOptimize.optimizeWrite = true
# MAGIC   );
# MAGIC   CREATE TABLE IF NOT EXISTS `raw_dbx_patterns_control`.`etl_audit`
# MAGIC   (
# MAGIC     total_count bigint,
# MAGIC     valid_count bigint,
# MAGIC     invalid_count bigint,
# MAGIC     invalid_ratio double,
# MAGIC     expected_row_count bigint,
# MAGIC     _process_id bigint,
# MAGIC     _load_date timestamp,
# MAGIC     file_name string,
# MAGIC     file_path string,
# MAGIC     file_size bigint,
# MAGIC     file_modification_time timestamp
# MAGIC   )
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/datalake/data/raw/raw_dbx_patterns_control/etl_audit'
# MAGIC   TBLPROPERTIES (
# MAGIC     delta.appendOnly = true,
# MAGIC     delta.autoOptimize.autoCompact = true,
# MAGIC     delta.autoOptimize.optimizeWrite = true
# MAGIC   )

# COMMAND ----------

# build a list of notebooks to run
task_root = "."
params = {"process_id": str(param_process_id)}
notebooks = [
  Notebook(
    path=f"{task_root}/autoload_raw_schema", 
    parameters={"process_id": str(param_process_id), "table": t.name, "cleardown": 0}, 
    timeout=param_timeout, 
    retry=0, 
    enabled=True) for t in tables
]

# execute the notebooks in parallel
results = execute_notebooks(
  notebooks=notebooks, 
  maxParallel=param_max_parallel, 
  dbutils=dbutils
)


# COMMAND ----------


msg = "\n".join(results)
print(msg)
