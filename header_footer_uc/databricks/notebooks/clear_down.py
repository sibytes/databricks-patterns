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
