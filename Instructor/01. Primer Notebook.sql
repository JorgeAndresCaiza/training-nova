-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <image src="https://storagepocdemos.blob.core.windows.net/public/images/databricks_banner.png?sp=r&st=2025-09-07T16:20:01Z&se=2027-01-01T04:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=93gWaOconS54rPMBkuUQ5hq9ji6GVlLPofBBZeI7yYI%3D" width="100%" alt="handytec"></image>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Revisión de Interfaz de Usuario

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Manejo de Markdown

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Titulo 4
-- MAGIC
-- MAGIC Este es un ejemplo de notebook generado
-- MAGIC
-- MAGIC *Texto Formateado*
-- MAGIC
-- MAGIC > Este es un ejemplo de cita de Bloque
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC `Bloque de código`
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ```
-- MAGIC {
-- MAGIC   "firstName": "John",
-- MAGIC   "lastName": "Smith",
-- MAGIC   "age": 25
-- MAGIC }
-- MAGIC ```
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC Ejemplo de Oración con nota de referencia. [^1]
-- MAGIC
-- MAGIC [^1]: Esta es la referencia
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ~~The world is flat.~~
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC 1. First item
-- MAGIC 2. Second item
-- MAGIC 3. Third item
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC - First item
-- MAGIC - Second item
-- MAGIC - Third item
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **`Desarrollador`**: Andrés Caiza
-- MAGIC | Syntax | Description |
-- MAGIC | ----------- | ----------- |
-- MAGIC | REF: 01 | Se agrega el campo aux. |
-- MAGIC | REF: 02 | Se elimina query auxiliar para generación de tabla final. |
-- MAGIC | REF: 03 | Se agrega el campo IdOrden. |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - [x] Write the press release
-- MAGIC - [ ] Update the website
-- MAGIC - [ ] Contact the media

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Uso de Magic Commands

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print("Hola Databricks")
-- MAGIC a = ["1","2","3"]
-- MAGIC print(a * 3)
-- MAGIC print(a[1])

-- COMMAND ----------

SELECT 1 as D, now() horaUTC, from_utc_timestamp(now(), 'America/Lima') horaActual, TRIM('   !"$·"·sdsadas    ')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ejemplo de Creación de Esquemas

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dbw_edwh_training.db_bronze;
CREATE DATABASE IF NOT EXISTS dbw_edwh_training.db_silver;
CREATE DATABASE IF NOT EXISTS dbw_edwh_training.db_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ejemplo de creación de tablas

-- COMMAND ----------

DROP TABLE IF EXISTS dbw_edwh_training.db_bronze.payment;
CREATE TABLE dbw_edwh_training.db_bronze.payment(
  payment_no bigint,
  member_no bigint,
  payment_dt timestamp,
  payment_amt double,
  statement_no bigint,
  payment_code string
);

DROP TABLE IF EXISTS dbw_edwh_training.db_bronze.statement;
CREATE TABLE dbw_edwh_training.db_bronze.statement(
  statement_no bigint,
  member_no bigint,
  statement_dt timestamp,
  due_dt timestamp,
  statement_amt bigint,
  statement_code string
)
