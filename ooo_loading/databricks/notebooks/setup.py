# Databricks notebook source
dbutils.widgets.text("project", "ooo_loading")
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
path = f"abfss://{catalog}@{storage_account}.dfs.core.windows.net/data/checkpoint/{project}/"
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

# DBTITLE 1,Create Data Volume
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

data_dir = os.path.join(home, f"../../data")

dbfs_from_path = f"file:{data_dir}"
# print(dbfs_from_path)
dbutils.fs.ls(dbfs_from_path)

print(f"Copying data from {dbfs_from_path} to {volume_path}")
dbutils.fs.cp(dbfs_from_path, volume_path, True)



# COMMAND ----------

# DBTITLE 1,Check Landing Data
volume_path = f"/Volumes/{catalog}/{container}/{project}/balance"
data_files = dbutils.fs.ls(volume_path)
dirs = [d.name for d in data_files]

assert len(dirs) == 10, f"balance file count is wrong, {len(dirs)} != 10"


# COMMAND ----------

# DBTITLE 1,Clear Down the Data Lakehouse
def clear_down():
  print(f"dropping database {catalog}.{project}")
  spark.sql(f"drop database if exists {catalog}.{project} CASCADE")

clear_down()

# COMMAND ----------


checkpoints = dbutils.fs.ls("/Volumes/development/checkpoint/ooo_loading")

for c in checkpoints:
  dbutils.fs.rm(c.path, True)

