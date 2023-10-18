# Databricks notebook source
# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  checkpoints = [
    "/mnt/datalake/checkpoint/ad_works_lt"
  ]
  for c in checkpoints:
    dbutils.fs.rm(c, True)
  spark.sql("drop database if exists raw_ad_works_lt CASCADE")
  spark.sql("drop database if exists base_ad_works_lt CASCADE")
  spark.sql("drop database if exists control_ad_works_lt CASCADE")
clear_down()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
