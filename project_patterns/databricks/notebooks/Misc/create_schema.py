# Databricks notebook source
paths = [
  {
    "name": "customer_details_1",
    "path": "/mnt/datalake/data/landing/dbx_patterns/customer_details_1/20230101/customer_details_1-20230101.csv"},
  {
    "name": "customer_details_2",
    "path": "/mnt/datalake/data/landing/dbx_patterns/customer_details_2/20230101/customer_details_2-20230101.csv"}
]



# COMMAND ----------

path = paths[0]
path

options = {
  "inferSchema": True,
  "mode": "PERMISSIVE",
  "header": True
}
format = "csv"

df = spark.read.format(format).options(**options).load(path["path"])
df.schema
