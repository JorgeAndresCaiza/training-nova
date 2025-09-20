# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

variable = dbutils.widgets.get("Var")
print(variable)

# COMMAND ----------

var='2025-12-01'
spark.conf.set("param.var",var)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${param.var}'

# COMMAND ----------

df = spark.sql("SELECT 'EmpresaA', 'EmpresaB', 'EmpresaC','EmpresaD','EmpresaE','EmpresaF'")

dbutils.widgets.text('fecha_corte', '', '1. Fecha Corte')
dbutils.widgets.multiselect('empresa', '*', choices=['*'] + df.columns, label='2. Empresa')
dbutils.widgets.dropdown('tipo_ejecucion', 'Stock', ['Stock','History'], '3. Tipo Ejecución')
dbutils.widgets.combobox('is_initial', 'True', ['True','False'], '4. Es carga inicial')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT :fecha_corte

# COMMAND ----------

dbutils.notebook.run('Instructor/04. Funciones y Transformaciones',3600,{'fecha_corte':'2025-09-20'})
