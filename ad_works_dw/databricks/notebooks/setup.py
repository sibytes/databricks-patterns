# Databricks notebook source
dbutils.widgets.text("project", "ad_works_dw")
dbutils.widgets.text("catalog", "development")
dbutils.widgets.text("storage_account", "datalakegeneva")
dbutils.widgets.text("container", "landing")  

# COMMAND ----------

project = dbutils.widgets.get("project")
catalog = dbutils.widgets.get("catalog")
storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")

# COMMAND ----------

# DBTITLE 1,Create Checkpoint Volume
path = f"abfss://{catalog}@{storage_account}.dfs.core.windows.net/data/checkpoint/{project}"
print(f"Creating volume {path}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.checkpoint")

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.checkpoint.{project}
LOCATION '{path}'
""")

# COMMAND ----------

volume_exists = (
  spark.sql(f"SHOW VOLUMES in {catalog}.checkpoint").where(f"volume_name = '{project}'")
).count()

assert volume_exists == 1, "volume can't be found"

# COMMAND ----------

# DBTITLE 1,Create Landing Volume
path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/{project}"
print(f"Creating volume {path}")

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{container}.{project}
LOCATION '{path}'
""")

# COMMAND ----------

volume_exists = (
  spark.sql(f"SHOW VOLUMES in {catalog}.{container}").where(f"volume_name = '{project}'")
).count()

assert volume_exists == 1, "volume can't be found"

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

spark.sql(f"drop database if exists raw_{project} CASCADE")
spark.sql(f"drop database if exists base_{project} CASCADE")
spark.sql(f"drop database if exists control_{project} CASCADE")

