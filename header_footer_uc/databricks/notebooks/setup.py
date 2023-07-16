# Databricks notebook source
# DBTITLE 1,Clear Landing
from pyspark.sql.utils import AnalysisException


dbfs_to_path = "/Volumes/development/landing/header_footer_uc/"
try:
  dbutils.fs.rm(dbfs_to_path, True)
except AnalysisException as e:
  if "UC_VOLUME_NOT_FOUND" in str(e):
    spark.sql("CREATE VOLUME development.landing.header_footer_uc;")
  raise e

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW VOLUMES in DEVELOPMENT.landing

# COMMAND ----------

# DBTITLE 1,Load Landing
import os

home = os.getcwd()

if home == '/databricks/driver':
  home = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  home = f"Workspace{home}"

data_dir = os.path.join(home, "../../../data/landing/customer_details")

dbfs_from_path = f"file:/{data_dir}"
print(dbfs_from_path)
dbutils.fs.ls(dbfs_from_path)

print(f"Copying data from {dbfs_from_path} to {dbfs_to_path}")
dbutils.fs.cp(dbfs_from_path, dbfs_to_path, True)



# COMMAND ----------

# DBTITLE 1,Check Landing Data
data_files = dbutils.fs.ls(dbfs_to_path)
dirs = [d.name.replace("/", "") for d in data_files]

assert "customer_details_1" in dirs, "customer_details_1 is missing, landing not setup"
assert "customer_details_2" in dirs, "customer_details_2 is missing, landing not setup"
assert "customer_preferences" in dirs, "customer_preferences is missing, landing not setup"
display(data_files)

# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  checkpoints = [
    "/mnt/datalake/checkpoint/header_footer_uc"
  ]
  for c in checkpoints:
    dbutils.fs.rm(c, True)
  spark.sql("drop database if exists yetl_raw_header_footer_uc CASCADE")
  spark.sql("drop database if exists yetl_base_header_footer_uc CASCADE")
  spark.sql("drop database if exists yetl_control_header_footer_uc CASCADE")
clear_down()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
