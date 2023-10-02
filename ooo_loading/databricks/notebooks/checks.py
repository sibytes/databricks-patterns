# Databricks notebook source
# MAGIC %sql
# MAGIC select * from development.yetl_raw_header_footer_uc.customer_details_1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from development.yetl_raw_header_footer_uc.customer_details_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from development.yetl_raw_header_footer_uc.customer_preferences

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct file_name from development.yetl_control_header_footer_uc.header_footer
# MAGIC -- where file_name = "customer_details_1-20230101.csv"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from development.yetl_control_header_footer_uc.raw_audit
