# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.2.4

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("table", "customer_details_1")

# COMMAND ----------

from yetl import (
  Config, Timeslice, StageType, Read, DeltaLake, ValidationThreshold
)
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StreamingQuery
import os
from typing import Union, Dict

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_table = dbutils.widgets.get("table")

print(f"""
  param_process_id: {param_process_id}
  param_table: {param_table}
""")


# COMMAND ----------

timeslice = Timeslice(day="*", month="*", year="*")
project = "project_patterns"
pipeline = "autoload_raw_schema"

config = Config(
  project=project, 
  pipeline=pipeline,
  timeslice=timeslice
)

# COMMAND ----------

# Load the data
from pipelines import load

table_mapping = config.get_table_mapping(
  stage=StageType.raw, 
  table=param_table
)
config.set_checkpoint(
  table_mapping.source, table_mapping.destination
)
print(table_mapping.destination.options["checkpointLocation"])
load(
  param_process_id, table_mapping.source, table_mapping.destination
)

# COMMAND ----------

# load the headers and footers
from pipelines import load_header_footer

table_mapping_hf = config.get_table_mapping(
  stage=StageType.audit_control, 
  table="header_footer"
)

source_hf:DeltaLake = table_mapping_hf.source[table_mapping.source.table]
config.set_checkpoint(
  source_hf, 
  table_mapping_hf.destination
)
print(table_mapping_hf.destination.options["checkpointLocation"])

load_header_footer(
  source_hf, 
  table_mapping_hf.destination
)

# COMMAND ----------

# load the audit table
from pipelines import load_audit

table_mapping_audit = config.get_table_mapping(
  stage=StageType.audit_control, 
  table="raw_audit"
)

source_audit:DeltaLake = table_mapping_audit.source[table_mapping.source.table]
source_hf:DeltaLake = table_mapping_audit.source["header_footer"]

load_audit(
  param_process_id, 
  source_audit, 
  table_mapping_hf.destination, 
  table_mapping_audit.destination
)

# COMMAND ----------

msg = f"Succeeded: {table_mapping.destination.database}.{table_mapping.destination.table}"
dbutils.notebook.exit(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from raw_dbx_patterns.customers
# MAGIC -- select * from raw_dbx_patterns.customer_details_1
# MAGIC select * from raw_dbx_patterns.customer_details_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), `_metadata`.file_name  from raw_dbx_patterns.customers group by `_metadata`.file_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns_control.header_footer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_dbx_patterns_control.raw_audit
