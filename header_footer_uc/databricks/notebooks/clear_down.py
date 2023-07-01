# Databricks notebook source
# DBTITLE 1,Clear Landing
# dbfs_to_path = "/mnt/landing/data/header_footer_uc"
# dbutils.fs.rm(dbfs_to_path, True)

# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  checkpoints = [
    "/mnt/datalake/data/yetl_raw_header_footer_uc",
    "/mnt/datalake/data/yetl_base_header_footer_uc",
    "/mnt/datalake/data/yetl_control_header_footer_uc",
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
