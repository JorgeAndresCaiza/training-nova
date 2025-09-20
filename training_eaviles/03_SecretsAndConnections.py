# Databricks notebook source
print('--- CREDENCIALES ---')
dbutils.credentials.help()
print('--- FILE SYSTEM ---')
dbutils.fs.help()
print('--- SECRETS ---')
dbutils.secrets.help()
print('--- WIDGETS ---')
dbutils.widgets.help()
print('--- NOTEBOOKS ---')
dbutils.notebook.help()

# COMMAND ----------

scope = 'kv-edwh-training'
ip = dbutils.secrets.get(scope = scope, key = 'host')
port = dbutils.secrets.get(scope = scope, key = 'port')
user = dbutils.secrets.get(scope = scope, key = 'user')
pwd = dbutils.secrets.get(scope = scope, key = 'pass')
db = dbutils.secrets.get(scope = scope, key = 'db')

connectionString = f"jdbc:sqlserver://{ip}:{port};database={db};user={user};password={pwd};trustServerCertificate=true"
connectionProperties = {
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "user": user,
  "password": pwd
}

# COMMAND ----------

query = """
SELECT * FROM SalesLT.SalesOrderDetail
"""
df = spark.read.jdbc(url = connectionString, table = f"({query}) AS t", properties = connectionProperties)
output_table_name = f"dbw_edwh_training.db_bronze.sales_order_detail_eaviles"
df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(output_table_name)
