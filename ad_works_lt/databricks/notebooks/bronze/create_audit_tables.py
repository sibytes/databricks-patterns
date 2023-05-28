# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.3.7

# COMMAND ----------
dbutils.widgets.text("load_type", "autoloader")


# COMMAND ----------

from etl import LoadType
from yetl import (
  Config, StageType
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

project = "header_footer"
pipeline = load_type.value

config = Config(
  project=project, 
  pipeline=pipeline
)

# COMMAND ----------

tables = config.tables.create_table(
  stage=StageType.audit_control, 
  first_match=False
)


# COMMAND ----------

dbutils.notebook.exit("Success")
