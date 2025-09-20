# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <image src="https://storagepocdemos.blob.core.windows.net/public/images/databricks_banner.png?sp=r&st=2025-09-07T16:20:01Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=93gWaOconS54rPMBkuUQ5hq9ji6GVlLPofBBZeI7yYI%3D" width="100%" alt="handytec"></image>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Cálculo de reservas de riesgo
# MAGIC
# MAGIC El ejercicio consiste en implementar una tabla (o vista) en la base de datos temporal (db_silver) que almacene el cálculo de "Reservas de riesgo" para las pólizas de la tabla `lakehouse_gtyt_dev.db_tmp.insurance`, tal como se ilustra en el siguiente archivo:
# MAGIC
# MAGIC [Ejemplo y requerimientos](https://storagepocdemos.blob.core.windows.net/public/documents/ejercicio_reserva_riesgo.xlsx?sp=r&st=2025-09-16T20:43:15Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=D3MiBJF89iY3RjWKikbL%2Fzb7w%2By3NODIeQis8rrZ%2FuM%3D)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla Base

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbw_edwh_training.db_bronze.insurance
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolución de problema con PySpark

# COMMAND ----------

from pyspark.sql.functions import col,explode,year,month,expr,months_between,ceil,when

# COMMAND ----------

#Leer tabla como un Spark Dataframe
insurance = spark.read.table("dbw_edwh_training.db_bronze.insurance")

insurance_date_series = insurance.withColumn("time_series", expr("""sequence(date_from, date_to, interval 1 month)"""))\
    .withColumn("int_months_between_dates", ceil(months_between(col("date_to"),col("date_from"), False)))\
    .withColumn("release_amount", col("withheld_amount")/col("int_months_between_dates"))\
    .withColumn("date_aux", explode(col("time_series")))\
    .withColumn("aux_year",year(col("date_aux")))\
    .withColumn("aux_month",month(col("date_aux")))\
    .withColumn("aux_year_acc",year(col("date_acc")))\
    .withColumn("aux_month_acc",month(col("date_acc")))\
    .withColumn("constitution_amount", when((col("aux_year") == col("aux_year_acc")) & (col("aux_month")==col("aux_month_acc")), col("withheld_amount")*-1)
                                    .otherwise(0))\
    .where(col("date_to")!=col("date_aux"))\
    .selectExpr("insurance_id","date_from","date_to","date_acc","withheld_amount","date_aux","aux_year","aux_month","constitution_amount","CAST(release_amount AS DECIMAL(28,2)) release_amount")\
    .filter("insurance_id='6085232'")

display(insurance_date_series)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolución de problema con SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_insurance_eaviles AS
# MAGIC SELECT *,
# MAGIC explode(sequence(date_from, DATEADD(month, -1, date_to), INTERVAL 1 MONTH)) date_aux
# MAGIC FROM dbw_edwh_training.db_bronze.insurance 
# MAGIC WHERE insurance_id = 6085232;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC YEAR(date_aux) aux_year,
# MAGIC MONTH(date_aux) aux_month,
# MAGIC CASE WHEN (YEAR(date_acc) = YEAR(date_aux) AND MONTH(date_acc) = MONTH(date_aux))
# MAGIC THEN (withheld_amount * -1) ELSE 0 END constitution_amount,
# MAGIC (withheld_amount/DATEDIFF(MONTH, date_from, date_to)) release_amount
# MAGIC FROM tmp_insurance_eaviles

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolución de problema con SQL - Forma 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_acc,withheld_amount/months_between(date_to, date_from) as release_amounth,explode(sequence(date_from,add_months(date_to,-1), INTERVAL 1 MONTH)) as fecha_pol,if(year(fecha_pol)||month(fecha_pol)=year(date_acc)||month(date_acc),withheld_amount*-1,0) from db_bronze.insurance
# MAGIC where insurance_id='6085232';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolución de problema con PySpark - Forma 2

# COMMAND ----------

from pyspark.sql import functions as F
 
df = spark.read.table("dbw_edwh_training.db_bronze.insurance")
# 1. Generar meses desde date_from hasta (date_to - 1 mes)
df_expanded = (
    df.withColumn(
        "month_seq",
        F.explode(
            F.sequence(
                F.trunc("date_from", "month"),
                F.add_months(F.trunc("date_to", "month"), -1),
                F.expr("interval 1 month")
            )
        )
    )
)
 
# 2. Calcular número de meses (sin último mes)
df_expanded = df_expanded.withColumn(
    "total_months",
    F.months_between(
        F.add_months(F.trunc("date_to", "month"), -1),
        F.trunc("date_from", "month")
    ).cast("int") + 1
)
 
# 3. Agregar columnas auxiliares
df_expanded = (
    df_expanded
    .withColumn("month_number",
                (F.months_between("month_seq", F.trunc("date_from", "month")) + 1).cast("int"))
    .withColumn("year", F.year("month_seq"))
    .withColumn("month", F.month("month_seq"))
)
 
# 4. withheld_amount_adj con coincidencia de año y mes
df_expanded = df_expanded.withColumn(
    "withheld_amount_adj",
    F.when(
        (F.year("date_acc") == F.col("year")) & (F.month("date_acc") == F.col("month")),
        F.col("withheld_amount") * -1
    ).otherwise(F.lit(0))
)
 
# 5. release_amount = withheld_amount / total_months
df_expanded = df_expanded.withColumn(
    "release_amount",
    (F.col("withheld_amount") / F.col("total_months"))
).filter("insurance_id='6085232'")
display(df_expanded)
 
# df_expanded.show(truncate=False)
 
# df_expanded.write.mode("overwrite").saveAsTable("dbw_edwh_training.db_bronze.nueva_tabla")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolución de problema con SQL - Alternativa

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT withheld_amount/months_between(date_to, date_from),explode(sequence(date_from, date_to, INTERVAL 1 MONTH)) as fecha_pol,if(year(fecha_pol)||month(fecha_pol)=year(date_acc)||month(date_acc),withheld_amount*-1,0) from db_bronze.insurance
# MAGIC where insurance_id='6085232';
# MAGIC  
