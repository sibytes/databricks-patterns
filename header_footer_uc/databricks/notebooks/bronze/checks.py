# Databricks notebook source
# MAGIC %sql
# MAGIC select * from raw_header_footer_uc.customer_details_1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_header_footer_uc.customer_details_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_header_footer_uc.customer_preferences

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from control_header_footer_uc.header_footer
# MAGIC -- where footer.name = "customer_preferences"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from control_header_footer_uc.raw_audit
