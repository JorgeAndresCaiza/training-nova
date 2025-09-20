# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <image src="https://storagepocdemos.blob.core.windows.net/public/images/databricks_banner.png?sp=r&st=2025-09-07T16:20:01Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=93gWaOconS54rPMBkuUQ5hq9ji6GVlLPofBBZeI7yYI%3D" width="100%" alt="handytec"></image>

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga de Datos

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV desde URL

# COMMAND ----------

import pandas as pd

a="https://storagepocdemos.blob.core.windows.net/public/documents/insurance_sample.csv?sp=r&st=2025-09-08T20:22:14Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=uUEG5%2FuqAjs1VVs4ayO7O8%2F2hDViE%2BKGqbNJZlA2Kr0%3D"

df_pandas = pd.read_csv(a, sep=',', header='infer')

df = spark.createDataFrame(df_pandas)

display(df)

# df = spark.createDataFrame(df_pandas)
# df.write.mode("overwrite").saveAsTable("insurance")

# COMMAND ----------

# Leer CSV con pandas
df_pandas = pd.read_csv(a, sep=',', header='infer')

# Convertir Dataframes de Pandas a Dataframe Spark
df = spark.createDataFrame(df_pandas);

# display(df)

# Guardar como vista temporal
df.createOrReplaceTempView("tmp_insurance")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dbw_edwh_training.db_bronze.insurance as
# MAGIC select 
# MAGIC   poliza as insurance_id,
# MAGIC   date(fecInicioVigencia) as date_from, 
# MAGIC   date(fecFinVigencia) as date_to, 
# MAGIC   date(fecContable) as date_acc, 
# MAGIC   montoRetenido as withheld_amount 
# MAGIC from tmp_insurance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM dbw_edwh_training.db_bronze.insurance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tmp_insurance

# COMMAND ----------

# MAGIC %md
# MAGIC ###CSV Desde Volumen
# MAGIC

# COMMAND ----------

path="/Volumes/dbw_edwh_training/db_bronze/input_files/Ejemplo01.csv"
df = spark.read.csv(path = path, header=True, encoding='UTF-8',inferSchema=True, sep=',', multiLine=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Excel Usando Pandas

# COMMAND ----------

import pandas as pd
path = "/Volumes/dbw_edwh_training/db_bronze/input_files/plantilla_parametros.xlsx"
pandas_df = pd.read_excel(path, sheet_name='plantilla_parametros')

display(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Excel Usando Liberias Externas Crealytics

# COMMAND ----------

path = "/Volumes/dbw_edwh_training/db_bronze/input_files/plantilla_parametros.xlsx"
sheet_name = "plantilla_parametros!A1"
schema=None
# schema="IdDeGrupo STRING, IdDeIngesta STRING, OrdenDeGrupo INTEGER, IdDeSecreto STRING, Conexion STRING , TablaFuente STRING"

if schema:
    df = spark.read.format("com.crealytics.spark.excel").option("inferSchema", "false").load(path=path, header='true', dataAddress=sheet_name, schema=schema)
else:
    df = spark.read.format("com.crealytics.spark.excel").option("inferSchema", "true").load(path=path, header='true', dataAddress=sheet_name, schema=schema)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dar permisos al grupo de capacitacion para que pueda usar la librería Crealytics

# COMMAND ----------

# MAGIC %sql
# MAGIC --Necesario permisos de administracion en el workspace
# MAGIC GRANT SELECT ON ANY FILE TO capacitacion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejemplo de Carga Nativa de JSON

# COMMAND ----------

from pyspark.sql.functions import explode,col

path = "/Volumes/dbw_edwh_training/db_bronze/input_files/OrdersData.json"

json_df = (spark.read.format("json")
                    .option("multiline", True)
                    .load(path))

# display(json_df)

json_df_aux = json_df.select(explode(json_df.data).alias("data")).selectExpr("data.*", "now()")

json_parsed_product_item = json_df_aux.select(explode(json_df_aux.productItems).alias("productItems")).selectExpr( "productItems.*")

# display(json_df)
# display(json_df_aux)
display(json_parsed_product_item)

# COMMAND ----------

# MAGIC %md
# MAGIC ###JSON Desde Campo String Usando Esquema

# COMMAND ----------

arrayData = [
        ('1','{"adjustedTax":6387,"basePrice":40000,"bonusProductLineItem":false,"brand":"M001","gift":false,"grossPrice":40000,"itemId":"fea6dd7fefc0974de4570473b7","itemText":"Nike Sportswear AeroBill Featherlight","minOrderQuantity":1,"netPrice":33613,"position":1,"priceAfterItemDiscount":40000,"priceAfterOrderDiscount":40000,"productId":"000000010018305002-3","productName":"Nike Sportswear AeroBill Featherlight","quantity":1,"shipmentId":"me","stepQuantity":1,"tax":6387,"taxBasis":40000,"taxRate":0.19},{"adjustedTax":80,"basePrice":500,"bonusProductLineItem":false,"brand":"M001","gift":false,"grossPrice":500,"itemId":"3f92ed5291b6e0d5d476ef9da7","itemText":"Nike Sportswear AeroBill Featherlight","minOrderQuantity":1,"netPrice":420,"position":2,"priceAfterItemDiscount":500,"priceAfterOrderDiscount":500,"productId":"10080956001","productName":"Nike Sportswear AeroBill Featherlight","quantity":1,"shipmentId":"me","stepQuantity":1,"tax":80,"taxBasis":500,"taxRate":0.19}'),
        ('2','{"adjustedTax":9580,"basePrice":60000,"bonusProductLineItem":false,"brand":"NIKE","gift":false,"grossPrice":60000,"itemId":"03d116dc3ddb310d5444d21246","itemText":"Nike Air Force 1 07","minOrderQuantity":1,"netPrice":50420,"position":1,"priceAfterItemDiscount":60000,"priceAfterOrderDiscount":60000,"productId":"000000010018305005","productName":"Nike Air Force 1 07","quantity":1,"shipmentId":"me","stepQuantity":1,"tax":9580,"taxBasis":60000,"taxRate":0.19}')
]

df_manual = spark.createDataFrame(data=arrayData, schema=['id','data'])
display(df_manual)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicion de esquema en JSON

# COMMAND ----------

from pyspark.sql.functions import explode,col,from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType, BooleanType, IntegerType, ArrayType

product_items_schema = StructType([
    StructField("adjustedTax", DoubleType(), True),
    StructField("basePrice", DoubleType(), True),
    StructField("bonusProductLineItem", BooleanType(), True),
    StructField("brand", StringType(), True),
    StructField("gift", BooleanType(), True),
    StructField("grossPrice", DoubleType(), True),
    StructField("itemId", StringType(), True),
    StructField("itemText", StringType(), True),
    StructField("minOrderQuantity", IntegerType(), True),
    StructField("netPrice", DoubleType(), True),
    StructField("position", IntegerType(), True),
    StructField("priceAdjustments", ArrayType(StructType([
                                            StructField("appliedDiscount", StructType([
                                                                                    StructField("amount", DoubleType(), True),
                                                                                    StructField("percentage", DoubleType(), True),
                                                                                    StructField("type", StringType(), True)
                                                                                ]), True),
                                            StructField("basePrice", DoubleType(), True),
                                            StructField("campaignId", StringType(), True),
                                            StructField("creationDate", TimestampType(), True),
                                            StructField("custom", BooleanType(), True),
                                            StructField("grossPrice", DoubleType(), True),
                                            StructField("itemText", StringType(), True),
                                            StructField("lastModified", TimestampType(), True),
                                            StructField("manual", BooleanType(), True),
                                            StructField("netPrice", DoubleType(), True),
                                            StructField("priceAdjustmentId", StringType(), True),
                                            StructField("promotionId", StringType(), True),
                                            StructField("tax", DoubleType(), True),
                                            StructField("taxBasis", DoubleType(), True)
                                        ])), True),
    StructField("priceAfterItemDiscount", DoubleType(), True),
    StructField("priceAfterOrderDiscount", DoubleType(), True),
    StructField("productId", StringType(), True),
    StructField("productName", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("shipmentId", StringType(), True),
    # StructField("stepQuantity", IntegerType(), True),
    # StructField("tax", DoubleType(), True),
    # StructField("taxBasis", DoubleType(), True),
    StructField("taxRate", DoubleType(), True)
])

df_parsed = df_manual.select(col("id"),from_json(col("data"), product_items_schema).alias("structured_data"))\
                     .selectExpr("id","structured_data.*")

display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reproceso completo de tabla - DROP AND CREATE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dbw_edwh_training.db_bronze.product_items

# COMMAND ----------

table_name = "dbw_edwh_training.db_bronze.product_items"

## Sin habilitación de esquemas dinámicos
# df_parsed.write.mode("overwrite").saveAsTable(table_name)
## Con habilitación de esquemas dinámicos
df_parsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualización de la tabla creada

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbw_edwh_training.db_bronze.product_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtener Metadatos Extendidos de Esquema y Ubicacion de la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED dbw_edwh_training.db_bronze.product_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borrado de registros de la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM dbw_edwh_training.db_bronze.product_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación de Historia de la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dbw_edwh_training.db_bronze.product_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restaruación de una tabla a una versión historica

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE dbw_edwh_training.db_bronze.product_items TO VERSION AS OF 0
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Procesos de Mantenimiento - VACUUM (Eliminar datos a nivel de storage que correspondan a versiones mas antiguas a 7 días -valor por default de retención de HISTORY en tablas delta-)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM dbw_edwh_training.db_bronze.product_items

# COMMAND ----------

# MAGIC %md
# MAGIC ### Procesos de Mantenimiento - OPTIMIZE (Compacta los archivos parquets en storage para agilizar la lectura de tablas delta)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dbw_edwh_training.db_bronze.product_items
