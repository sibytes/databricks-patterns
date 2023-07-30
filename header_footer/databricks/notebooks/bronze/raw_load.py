# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==2.0.4.dev2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("max_parallel", "4")
dbutils.widgets.text("timeout", "3600")
dbutils.widgets.text("process_group", "1")
dbutils.widgets.text("load_type", "batch")
dbutils.widgets.text("timeslice", "*")


# COMMAND ----------

from etl import LoadType
from yetl import (
  Config, Timeslice, StageType
)
from yetl.workflow import (
  execute_notebooks, Notebook
)

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_max_parallel = int(dbutils.widgets.get("max_parallel"))
param_timeout = int(dbutils.widgets.get("timeout"))
param_process_group = int(dbutils.widgets.get("process_group"))
param_load_type = dbutils.widgets.get("load_type")
param_timeslice = dbutils.widgets.get("timeslice")

try:
  load_type:LoadType = LoadType(param_load_type)
except Exception as e:
   raise Exception(f"load_type parameter {param_load_type} is not valid")

print(f"""
  param_process_id: {param_process_id}
  param_max_parallel: {param_max_parallel}
  param_timeout: {param_timeout}
  process_group: {param_process_group}
  load_type: {str(load_type)}
  timeslice: {param_timeslice}
""")

# COMMAND ----------

project = "header_footer"
pipeline = load_type.value

config = Config(
  project=project, 
  pipeline=pipeline
)

# COMMAND ----------

tables = config.lookup_table(
  stage=StageType.raw, 
  first_match=False,
  # this will filter the tables on a custom property
  # in the tables parameter you can add whatever custom properties you want
  # either for filtering or to use in pipelines
  process_group=param_process_group
)


# COMMAND ----------

# build a list of notebooks to run
task_root = "."
params = {"process_id": str(param_process_id)}
notebooks = [
  Notebook(
    path=f"{task_root}/load_table", 
    parameters={
      "process_id": str(param_process_id), 
      "table": t.table,
      "load_type": load_type.value
    }, 
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
