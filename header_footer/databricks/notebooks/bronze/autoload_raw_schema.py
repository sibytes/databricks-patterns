# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==1.3.0

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("table", "customer_details_1")

# COMMAND ----------

from yetl import (
  Config, Timeslice, StageType, DeltaLake
)

# COMMAND ----------

param_process_id = int(dbutils.widgets.get("process_id"))
param_table = dbutils.widgets.get("table")

print(f"""
  param_process_id: {param_process_id}
  param_table: {param_table}
""")


# COMMAND ----------

timeslice = Timeslice(day="*", month="*", year="*")
project = "header_footer"
pipeline = "autoload_raw_schema"

config = Config(
  project=project, 
  pipeline=pipeline,
  timeslice=timeslice
)

# COMMAND ----------

# Load the data
from etl import load

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
from etl import load_header_footer

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
from etl import load_audit

table_mapping_audit = config.get_table_mapping(
  stage=StageType.audit_control, 
  table="raw_audit"
)

source_audit:DeltaLake = table_mapping_audit.source[table_mapping.source.table]
source_hf:DeltaLake = table_mapping_audit.source["header_footer"]

landing = table_mapping.source
raw = table_mapping.destination
header_footer = table_mapping_audit.source["header_footer"]
destination = table_mapping_audit.destination

load_audit(
  param_process_id, 
  landing, 
  raw, 
  header_footer,
  destination
)

# COMMAND ----------

msg = f"Succeeded: {table_mapping.destination.database}.{table_mapping.destination.table}"
dbutils.notebook.exit(msg)
