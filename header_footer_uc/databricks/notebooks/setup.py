# Databricks notebook source
dbutils.widgets.text("project", "header_footer_uc")
dbutils.widgets.text("catalog", "development")
dbutils.widgets.text("storage_account", "datalakegeneva")
dbutils.widgets.text("container", "landing")

# COMMAND ----------

project = dbutils.widgets.get("project")
catalog = dbutils.widgets.get("catalog")
storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")

path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/{project}"
print(f"Creating volume {path}")

# COMMAND ----------

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

# DBTITLE 1,Clear Landing
volume_path = f"/Volumes/{catalog}/{container}/{project}/"
dbutils.fs.rm(volume_path, True)

# COMMAND ----------

# DBTITLE 1,Load Landing
import os

home = os.getcwd()

if home == '/databricks/driver':
  home = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  home = f"/Workspace{home}"

data_dir = os.path.join(home, f"../../../data/{container}/customer_details")

dbfs_from_path = f"file:{data_dir}"
print(dbfs_from_path)
dbutils.fs.ls(dbfs_from_path)

print(f"Copying data from {dbfs_from_path} to {volume_path}")
dbutils.fs.cp(dbfs_from_path, volume_path, True)



# COMMAND ----------

# DBTITLE 1,Check Landing Data
data_files = dbutils.fs.ls(volume_path)
dirs = [d.name.replace("/", "") for d in data_files]

assert "customer_details_1" in dirs, f"customer_details_1 is missing, {container} not setup"
assert "customer_details_2" in dirs, f"customer_details_2 is missing, {container} not setup"
assert "customer_preferences" in dirs, f"customer_preferences is missing, {container} not setup"
display(data_files)

# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  print(f"dropping database {catalog}.yetl_raw_{project}")
  spark.sql(f"drop database if exists {catalog}.yetl_raw_{project} CASCADE")
  print(f"dropping database {catalog}.yetl_base_{project}")
  spark.sql(f"drop database if exists {catalog}.yetl_base_{project} CASCADE")
  print(f"dropping database {catalog}.yetl_control_{project}")
  spark.sql(f"drop database if exists {catalog}.yetl_control_{project} CASCADE")
clear_down()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases
