# Databricks notebook source
dbutils.widgets.text("project", "header_footer_uc")
dbutils.widgets.text("catalog", "development")
dbutils.widgets.text("container", "landing")

# COMMAND ----------

project = dbutils.widgets.get("project")
catalog = dbutils.widgets.get("catalog")
container = dbutils.widgets.get("container")

# COMMAND ----------

# DBTITLE 1,Clear Landing
volume_path = f"/Volumes/{catalog}/{container}/{project}/"
dbutils.fs.rm(volume_path, True)


# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  spark.sql(f"drop database if exists development.yetl_raw_{project} CASCADE")
  spark.sql(f"drop database if exists development.yetl_base_{project} CASCADE")
  spark.sql(f"drop database if exists development.yetl_control_{project} CASCADE")
clear_down()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
