-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Primer Notebook Training
-- MAGIC Ricardo Herrera
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC a=5
-- MAGIC b=[1,2]
-- MAGIC print(a)
-- MAGIC print(b)

-- COMMAND ----------

SELECT
  run_notebook_completed_metadata.notebook_name AS notebook_name,
  COUNT(*) AS run_count
FROM system.access.clean_room_events
WHERE event_type = 'RUN_NOTEBOOK_COMPLETED'
  AND run_notebook_completed_metadata.state = 'SUCCESS'
GROUP BY run_notebook_completed_metadata.notebook_name
ORDER BY run_count DESC
LIMIT 1
