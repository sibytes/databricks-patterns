# Databricks notebook source
# DBTITLE 1,Clear Landing
# dbfs_to_path = "/mnt/landing/data/header_footer"
# dbutils.fs.rm(dbfs_to_path, True)

# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  checkpoints = [
    "/mnt/datalake/data/raw/raw_ad_works_dw",
    "/mnt/datalake/data/base/base_ad_works_dw",
    "/mnt/datalake/data/control/control_ad_works_dw",
    "/mnt/datalake/checkpoint/ad_works_dw"
  ]
  for c in checkpoints:
    dbutils.fs.rm(c, True)
  spark.sql("drop database if exists raw_ad_works_dw CASCADE")
  spark.sql("drop database if exists base_ad_works_dw CASCADE")
  spark.sql("drop database if exists control_ad_works_dw CASCADE")
clear_down()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
