# Databricks notebook source
# DBTITLE 1,Clear Landing
# dbfs_to_path = "/mnt/landing/data/header_footer"
# dbutils.fs.rm(dbfs_to_path, True)

# COMMAND ----------

# DBTITLE 1,Load Landing
# import os

# home = os.getcwd()
# print(home)

# data_dir = os.path.join(home, "../../../data/landing/customer_details")

# dbfs_from_path = f"file://{data_dir}"
# dbutils.fs.ls(dbfs_from_path)

# print(f"Copying data from {dbfs_from_path} to {dbfs_to_path}")
# dbutils.fs.cp(dbfs_from_path, dbfs_to_path, True)



# COMMAND ----------

# DBTITLE 1,Check Landing Data
# data_files = dbutils.fs.ls(dbfs_to_path)
# dirs = [d.name.replace("/", "") for d in data_files]

# assert "customer_details_1" in dirs, "customer_details_1 is missing, landing not setup"
# assert "customer_details_2" in dirs, "customer_details_2 is missing, landing not setup"
# assert "customer_preferences" in dirs, "customer_preferences is missing, landing not setup"
# display(data_files)

# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  checkpoints = [
    "/mnt/datalake/data/raw/raw_ad_works_lt",
    "/mnt/datalake/data/base/base_ad_works_lt",
    "/mnt/datalake/data/control/control_ad_works_lt",
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
