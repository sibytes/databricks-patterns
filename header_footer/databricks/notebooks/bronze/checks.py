# Databricks notebook source
# MAGIC %sql
# MAGIC select * from yetl_raw_header_footer.customer_details_1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yetl_raw_header_footer.customer_details_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yetl_raw_header_footer.customer_preferences

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yetl_control_header_footer.header_footer
# MAGIC -- where footer.name = "customer_preferences"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yetl_control_header_footer.raw_audit
