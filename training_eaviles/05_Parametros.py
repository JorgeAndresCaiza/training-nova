# Databricks notebook source
my_var = dbutils.widgets.get("var_date")
print(my_var)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT :var_date

# COMMAND ----------

dbutils.notebook.run("training_eaviles/04_FunctionsAndTransformations", 60, {"var_date": "2025-09-01"})

# COMMAND ----------

other_var = '2025-09-01'
spark.conf.set("param.var", other_var)

# COMMAND ----------

# MAGIC %sql
# MAGIC select '${param.var}'
