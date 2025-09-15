-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Titulo1
-- MAGIC ## T2
-- MAGIC ### H3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Fecha | Descripcion del Cambio |
-- MAGIC | ----------- | ----------- |
-- MAGIC | 2025-09-15 | Añadido Markdown |
-- MAGIC | Paragraph | Text |
-- MAGIC
-- MAGIC
-- MAGIC 	Here's a sentence with a footnote. [^1]
-- MAGIC   
-- MAGIC
-- MAGIC [^1]: This is the footnote.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - [x] Write the press release
-- MAGIC - [ ] Update the website
-- MAGIC - [ ] Contact the media

-- COMMAND ----------

-- MAGIC %py
-- MAGIC a=5
-- MAGIC b=[1,2]
-- MAGIC
-- MAGIC print(a)
-- MAGIC print(b*3)

-- COMMAND ----------

WITH job_run_counts AS (
  SELECT
    t1.workspace_id,
    t1.usage_metadata.job_id,
    COUNT(DISTINCT t1.usage_metadata.job_run_id) AS run_count
  FROM system.billing.usage t1
  WHERE
    t1.billing_origin_product = 'JOBS'
    AND t1.usage_metadata.job_id IS NOT NULL
    AND t1.workspace_id = 'dbw-edwh-training'
  GROUP BY t1.workspace_id, t1.usage_metadata.job_id
)
SELECT
  t2.name AS job_name,
  t1.job_id,
  t1.run_count
FROM job_run_counts t1
LEFT JOIN system.lakeflow.jobs t2
  ON t1.workspace_id = t2.workspace_id AND t1.job_id = t2.job_id
ORDER BY t1.run_count DESC
LIMIT 1;

-- COMMAND ----------

CREATE TABLE test (
  id INT,
  name STRING
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Titulo 2
