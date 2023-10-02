# Databricks notebook source
# DBTITLE 1,Clear Landing
# dbfs_to_path = "/mnt/landing/data/header_footer"
# dbutils.fs.rm(dbfs_to_path, True)

# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  checkpoints = [
    "/mnt/datalake/data/yetl_raw_ad_works_lt",
    "/mnt/datalake/data/yetl_base_ad_works_lt",
    "/mnt/datalake/data/yetl_control_ad_works_lt",
    "/mnt/datalake/checkpoint/yetl_ad_works_lt"
  ]
  for c in checkpoints:
    dbutils.fs.rm(c, True)
  spark.sql("drop database if exists yetl_raw_ad_works_lt CASCADE")
  spark.sql("drop database if exists yetl_base_ad_works_lt CASCADE")
  spark.sql("drop database if exists yetl_control_ad_works_lt CASCADE")
clear_down()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
