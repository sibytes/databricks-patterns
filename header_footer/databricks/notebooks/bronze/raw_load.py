# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.3.0

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

project = "header_footer"
pipeline = "autoload_raw_schema"

config = Config(
  project=project, 
  pipeline=pipeline
)

# COMMAND ----------

tables = config.tables.lookup_table(
  stage=StageType.raw, 
  first_match=False,
  process_group=param_process_group
)
msg_tables = '\n'.join([f"{t.database}.{t.table}" for t in tables])
print(f"{msg_tables}")

# COMMAND ----------

# build a list of notebooks to run
task_root = "."
params = {"process_id": str(param_process_id)}
notebooks = [
  Notebook(
    path=f"{task_root}/autoload_raw_schema", 
    parameters={"process_id": str(param_process_id), "table": t.table}, 
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

# COMMAND ----------

dbutils.notebook.exit("Success")
