# Databricks notebook source
# MAGIC %md
# MAGIC ###Código para Borrar Widgets de Notebook

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtener valor de widget creado manualmente en la secció Edit > Add Parameter 

# COMMAND ----------

variable = dbutils.widgets.get("Var")
print(variable)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inyección de variable en el contexto de Spark

# COMMAND ----------

var='2025-12-01'
spark.conf.set("param.var",var)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uso de variable en Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${param.var}'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de widgets mediante código

# COMMAND ----------

df = spark.sql("SELECT 'EmpresaA', 'EmpresaB', 'EmpresaC','EmpresaD','EmpresaE','EmpresaF'")

dbutils.widgets.text('fecha_corte', '', '1. Fecha Corte')
dbutils.widgets.multiselect('empresa', '*', choices=['*'] + df.columns, label='2. Empresa')
dbutils.widgets.dropdown('tipo_ejecucion', 'Stock', ['Stock','History'], '3. Tipo Ejecución')
dbutils.widgets.combobox('is_initial', 'True', ['True','False'], '4. Es carga inicial')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uso nativo de widgets en Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT :fecha_corte

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución de un notebook externo

# COMMAND ----------

dbutils.notebook.run('Instructor/04. Funciones y Transformaciones',3600,{'fecha_corte':'2025-09-20'})
