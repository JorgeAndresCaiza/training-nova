# Databricks notebook source
# MAGIC %py
# MAGIC import pandas as pd
# MAGIC CSV_URL="https://storagepocdemos.blob.core.windows.net/public/documents/insurance_sample.csv?sp=r&st=2025-09-08T20:22:14Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=uUEG5%2FuqAjs1VVs4ayO7O8%2F2hDViE%2BKGqbNJZlA2Kr0%3D"
# MAGIC
# MAGIC df_pandas=pd.read_csv(CSV_URL,sep=',', header='infer')
# MAGIC df = spark.createDataFrame(df_pandas)
# MAGIC df.createOrReplaceTempView("tmp_insurance")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dbw_edwh_training.db_bronze.insurance_eaviles AS
# MAGIC SELECT * FROM tmp_insurance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbw_edwh_training.db_bronze.insurance

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Cálculo de reservas de riesgo
# MAGIC
# MAGIC El ejercicio consiste en implementar una tabla (o vista) en la base de datos temporal (db_silver) que almacene el cálculo de "Reservas de riesgo" para las pólizas de la tabla `lakehouse_gtyt_dev.db_tmp.insurance`, tal como se ilustra en el siguiente archivo:
# MAGIC
# MAGIC [Ejemplo y requerimientos](https://storagepocdemos.blob.core.windows.net/public/documents/ejercicio_reserva_riesgo.xlsx?sp=r&st=2025-09-16T20:43:15Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=D3MiBJF89iY3RjWKikbL%2Fzb7w%2By3NODIeQis8rrZ%2FuM%3D)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *,
# MAGIC   explode(sequence(date_from, DATEADD(month, -1, date_to), INTERVAL 1 MONTH)) AS date_aux,
# MAGIC   YEAR(date_aux) aux_year,
# MAGIC   MONTH(date_aux) aux_month,
# MAGIC   CASE WHEN (YEAR(date_acc) = YEAR(date_aux) AND MONTH(date_acc) = MONTH(date_aux))
# MAGIC   THEN (withheld_amount * -1) ELSE 0
# MAGIC   END constitution_amount,
# MAGIC   (withheld_amount/DATEDIFF(MONTH, date_from, date_to)) release_amount
# MAGIC FROM dbw_edwh_training.db_bronze.insurance
# MAGIC WHERE insurance_id = 6085232;
