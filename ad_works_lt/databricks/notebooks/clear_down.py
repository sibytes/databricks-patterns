# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text("project", "ad_works_lt")
dbutils.widgets.text("catalog", "development")
dbutils.widgets.text("storage_account", "datalakegeneva")
dbutils.widgets.text("container", "landing")  

# COMMAND ----------

project = dbutils.widgets.get("project")
catalog = dbutils.widgets.get("catalog")
storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")


# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
volume_path = f"/Volumes/{catalog}/checkpoint/{project}/"
dbutils.fs.rm(volume_path, True)

print(f"dropping database {catalog}.raw_{project}")
spark.sql(f"drop database if exists {catalog}.raw_{project} CASCADE")
print(f"dropping database {catalog}.base_{project}")
spark.sql(f"drop database if exists {catalog}.base_{project} CASCADE")
print(f"dropping database {catalog}.control_{project}")
spark.sql(f"drop database if exists {catalog}.control_{project} CASCADE")

spark.sql("drop database if exists raw_ad_works_lt CASCADE")
spark.sql("drop database if exists base_ad_works_lt CASCADE")
spark.sql("drop database if exists control_ad_works_lt CASCADE")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
