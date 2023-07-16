# Databricks notebook source
# MAGIC %pip install pyaml pydantic yetl-framework==2.0.0.dev3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("process_id", "-1")
dbutils.widgets.text("table", "customer_details_1")
dbutils.widgets.text("load_type", "batch")
dbutils.widgets.text("timeslice", "*")

# COMMAND ----------

from yetl import (
  Config, Timeslice, StageType, DeltaLake
)
from etl import LoadType, LoadFunction, get_load

# COMMAND ----------

# DBTITLE 1,Handle & Validate Parameters
param_process_id = int(dbutils.widgets.get("process_id"))
param_table = dbutils.widgets.get("table")
param_load_type = dbutils.widgets.get("load_type")
param_timeslice = dbutils.widgets.get("timeslice")

try:
  load_type:LoadType = LoadType(param_load_type)
except Exception as e:
   raise Exception(f"load_type parameter {param_load_type} is not valid")

if load_type == LoadType.autoloader:
  param_timeslice = "*"
timeslice = Timeslice.parse_iso_date(param_timeslice)

print(f"""
  param_process_id: {param_process_id}
  param_table: {param_table}
  load_type: {str(load_type)}
  timeslice: {timeslice}
""")

# COMMAND ----------

project = "header_footer"
pipeline = load_type.value

config = Config(
  project=project, 
  pipeline=pipeline,
  timeslice=timeslice
)

# COMMAND ----------

# Load the data

load = get_load(LoadFunction.load, load_type)

table_mapping = config.get_table_mapping(
  stage=StageType.raw, 
  table=param_table,
  create_table=True
)
config.set_checkpoint(
  table_mapping.source, table_mapping.destination
)

# COMMAND ----------

print(load)
load(
  param_process_id, table_mapping.source, table_mapping.destination
)

# COMMAND ----------

# load the headers and footers

load = get_load(LoadFunction.load_header_footer, load_type)

table_mapping_hf = config.get_table_mapping(
  stage=StageType.audit_control, 
  table="header_footer"
)

source_hf:DeltaLake = table_mapping_hf.source[table_mapping.source.table]
config.set_checkpoint(
  source_hf, 
  table_mapping_hf.destination
)


# COMMAND ----------

print(load)

load(
  param_process_id,
  source_hf, 
  table_mapping_hf.destination
)

# COMMAND ----------

# load the audit table
load = get_load(LoadFunction.load_audit, load_type)

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



# COMMAND ----------

load(
  param_process_id, 
  landing, 
  raw, 
  header_footer,
  destination
)

# COMMAND ----------

msg = f"Succeeded: {table_mapping.destination.database}.{table_mapping.destination.table}"
dbutils.notebook.exit(msg)
