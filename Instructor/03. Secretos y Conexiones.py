# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <image src="https://storagepocdemos.blob.core.windows.net/public/images/databricks_banner.png?sp=r&st=2025-09-07T16:20:01Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=93gWaOconS54rPMBkuUQ5hq9ji6GVlLPofBBZeI7yYI%3D" width="100%" alt="handytec"></image>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilitarios Databricks Dbutils

# COMMAND ----------

dbutils.credentials.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uso de Keyvault
# MAGIC
# MAGIC -- Previamente se debe obtener los accesos al secret scope usando el api https://adb-942896811269292.12.azuredatabricks.net/api/2.0/secrets/acls/put
# MAGIC
# MAGIC Con el body:
# MAGIC {
# MAGIC   "scope": "kv-edwh-training",
# MAGIC   "principal": "capacitacion",
# MAGIC   "permission": "READ"
# MAGIC }
# MAGIC
# MAGIC La autenticación se realiza con el token extraido del usuario (Bearer Token)

# COMMAND ----------

scope= 'kv-edwh-training'
ip = dbutils.secrets.get(scope = scope, key = "host")
port = dbutils.secrets.get(scope = scope, key = "port")
db = dbutils.secrets.get(scope = scope, key = "db")
user = dbutils.secrets.get(scope = scope, key = "user")
password = dbutils.secrets.get(scope = scope, key = "pass")

connection_string= f"jdbc:sqlserver://{ip}:{port};database={db};trustServerCertificate=true"

connection_properties = {
    "user" : user,
    "password" : password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejemplo básico de ejecución de consulta hacia fuente de datos

# COMMAND ----------

query = """
SELECT
*
FROM SalesLT.SalesOrderDetail
"""
df = spark.read.jdbc(url=connection_string, table=f"({query}) AS t", properties=connection_properties)
# display(df)

output_table_name = f"dbw_edwh_training.db_bronze.sales_order_detail_acaiza"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(output_table_name)
