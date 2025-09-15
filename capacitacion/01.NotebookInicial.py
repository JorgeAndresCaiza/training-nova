# Databricks notebook source
# MAGIC %md
# MAGIC Titulo 1
# MAGIC

# COMMAND ----------

a=5
b=[1,2]
print(a)
print(b*3)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE my_table (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   value DOUBLE
# MAGIC )
# MAGIC USING DELTA
