# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <image src="https://storagepocdemos.blob.core.windows.net/public/images/databricks_banner.png?sp=r&st=2025-09-07T16:20:01Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=93gWaOconS54rPMBkuUQ5hq9ji6GVlLPofBBZeI7yYI%3D" width="100%" alt="handytec"></image>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carga De Datos

# COMMAND ----------

# MAGIC %md
# MAGIC ###CVS desde URL

# COMMAND ----------

!pip install openpyxl
import openpyxl as xlsx

# COMMAND ----------

import pandas as pd

a = "https://storagepocdemos.blob.core.windows.net/public/documents/insurance_sample.csv?sp=r&st=2025-09-08T20:22:14Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=uUEG5%2FuqAjs1VVs4ayO7O8%2F2hDViE%2BKGqbNJZlA2Kr0%3D"

df_pandas = pd.read_csv(a,sep=',',header = 'infer')

df = spark.createDataFrame(df_pandas)

display(df)

#df.write.mode("overwrite").saveAsTable("insurance")

# COMMAND ----------

path = "/Volumes/dbw_edwh_training/db_bronze/input_files_rh/Ejemplo01(in).csv"

df = spark.read.csv(path = path, header = True, encoding = 'UTF-8', inferSchema = True, sep=',', multiLine = True)

display(df)

# COMMAND ----------

path = "/Volumes/dbw_edwh_training/db_bronze/input_files_rh/plantilla_parametros 1.xlsx"

pandas_df = pd.read_excel(path, sheet_name = 'plantilla_parametros') #, engine = "openpyxls") #si es un excel antiguo

display(pandas_df)
#df = spark.createDataFrame(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Excel usando librerías externas Crealytics

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

from pyspark.sql.functions import explode

path = "/Volumes/dbw_edwh_training/db_bronze/input_files_rh/OrdersData.json"

#Cargar toda la carpeta (recomendado agrupar por carpetas los json con el mismo esquema):
#path = "/Volumes/dbw_edwh_training/db_bronze/input_files_rh/*.json"

json_df = spark.read.format("json")\ 
                    .option("multiline", True)\
                    .load(path) #Es útil \ para que reconozca saltos de línea

#display(json_df)

#json_df_aux = json_df.select(explode(json_df.data).alias("data")).selectExpr("data.*","now()")

json_df_aux = json_df.select(explode(json_df.data).alias("data")).selectExpr("data.billingAddress.*","now()")

#FALTA UNA LÍNEA 


display(json_df_aux)

# COMMAND ----------

arrayData = [
        ('1','{"adjustedTax":6387,"basePrice":40000,"bonusProductLineItem":false,"brand":"M001","gift":false,"grossPrice":40000,"itemId":"fea6dd7fefc0974de4570473b7","itemText":"Nike Sportswear AeroBill Featherlight","minOrderQuantity":1,"netPrice":33613,"position":1,"priceAfterItemDiscount":40000,"priceAfterOrderDiscount":40000,"productId":"000000010018305002-3","productName":"Nike Sportswear AeroBill Featherlight","quantity":1,"shipmentId":"me","stepQuantity":1,"tax":6387,"taxBasis":40000,"taxRate":0.19},{"adjustedTax":80,"basePrice":500,"bonusProductLineItem":false,"brand":"M001","gift":false,"grossPrice":500,"itemId":"3f92ed5291b6e0d5d476ef9da7","itemText":"Nike Sportswear AeroBill Featherlight","minOrderQuantity":1,"netPrice":420,"position":2,"priceAfterItemDiscount":500,"priceAfterOrderDiscount":500,"productId":"10080956001","productName":"Nike Sportswear AeroBill Featherlight","quantity":1,"shipmentId":"me","stepQuantity":1,"tax":80,"taxBasis":500,"taxRate":0.19}'),
        ('2','{"adjustedTax":9580,"basePrice":60000,"bonusProductLineItem":false,"brand":"NIKE","gift":false,"grossPrice":60000,"itemId":"03d116dc3ddb310d5444d21246","itemText":"Nike Air Force 1 07","minOrderQuantity":1,"netPrice":50420,"position":1,"priceAfterItemDiscount":60000,"priceAfterOrderDiscount":60000,"productId":"000000010018305005","productName":"Nike Air Force 1 07","quantity":1,"shipmentId":"me","stepQuantity":1,"tax":9580,"taxBasis":60000,"taxRate":0.19}')
]

df_manual = spark.createDataFrame(data=arrayData, schema=['id','data'])
display(df_manual)

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
    StructField("stepQuantity", IntegerType(), True),
    StructField("tax", DoubleType(), True),
    StructField("taxBasis", DoubleType(), True),
    StructField("taxRate", DoubleType(), True)
])

df_parsed = df_manual.select(col("id"),from_json(col("data"), product_items_schema).alias("structured_data"))\
                     .selectExpr("id","structured_data.*")

display(df_parsed)

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
    StructField("stepQuantity", IntegerType(), True), #
    StructField("tax", DoubleType(), True), #
    StructField("taxBasis", DoubleType(), True), #
    StructField("taxRate", DoubleType(), True)
])
 
df_parsed = df_manual.select(col("id"),from_json(col("data"), product_items_schema).alias("structured_data"))\
                     .selectExpr("id","structured_data.*")
 
display(df_parsed)

# COMMAND ----------

table_name = "dbw_edwh_training.db_bronze.product_items_rh"
 
# df_parsed.write.mode("overwrite").saveAsTable(table_name)
 
df_parsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbw_edwh_training.db_bronze.product_items_rh

# COMMAND ----------

# MAGIC %sql
# MAGIC --describe table dbw_edwh_training.db_bronze.product_items_rh
# MAGIC describe extended dbw_edwh_training.db_bronze.product_items_rh

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbw_edwh_training.db_bronze.product_items_rh version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table edwh_training.db_bronze.product_items_rh version as of 0; -- restaurar la tabla a la versión 0
# MAGIC
# MAGIC describe history edwh_training.db_bronze.product_items_rh; --ver historial de versiones
# MAGIC
# MAGIC vacuum edwh_training.db_bronze.product_items_rh;  --eliminar las versiones guardadas
# MAGIC
# MAGIC optimize edwh_training.db_bronze.product_items_rh; --minificar y unificar archivos pequeños (snappy parquet), para que sea má rápido de leer
