# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def raw_balance():

  return (
    spark.readStream.format("delta")
    .table("development.ooo_loading.raw_balance")
    .select(
      "id", 
      "code_1", 
      "code_2", 
      "code_3", 
      "code_4", 
      "code_5",
      "transacted", 
      "created", 
      "amount", 
      "qauntity", 
      "price", 
      "tax", 
      "value"
    )
  )

dlt.create_streaming_table("balance")

dlt.apply_changes(
  target = "balance",
  source = "raw_balance",
  keys = ["id", "code_1", "code_2", "code_3", "code_4", "code_5"],
  sequence_by = col("created"),
  # apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["created"],
  stored_as_scd_type = "2"
)

# COMMAND ----------

dbutils.notebook.exit("Finished")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   transacted,
# MAGIC   amount,
# MAGIC   qauntity,
# MAGIC   price,
# MAGIC   tax,
# MAGIC   value,
# MAGIC   `__START_AT`,
# MAGIC   `__END_AT`
# MAGIC from development.ooo_loading.balance
# MAGIC where 1=1
# MAGIC and `id` = 13
# MAGIC and `code_1` = '656684944-0'
# MAGIC and `code_2` = '267161185-4' 
# MAGIC and `code_3` = '036389464-0'
# MAGIC and `code_4` = '023012256-6'
# MAGIC and `code_5` = '989988643-2'
# MAGIC order by `__START_AT`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   transacted, 
# MAGIC   created, 
# MAGIC   amount, 
# MAGIC   qauntity, 
# MAGIC   price, 
# MAGIC   tax, 
# MAGIC   value
# MAGIC from `development`.`ooo_loading`.`raw_balance`
# MAGIC where 1=1
# MAGIC and `id` = 13
# MAGIC and `code_1` = '656684944-0'
# MAGIC and `code_2` = '267161185-4' 
# MAGIC and `code_3` = '036389464-0'
# MAGIC and `code_4` = '023012256-6'
# MAGIC and `code_5` = '989988643-2'
# MAGIC order by created

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC print(spark.sql("""
# MAGIC show create table development.ooo_loading.balance
# MAGIC """).first()[0])
