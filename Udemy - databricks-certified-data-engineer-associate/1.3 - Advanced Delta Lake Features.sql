-- Databricks notebook source
DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * FROM employees VERSION AS OF 1

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 2

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/")

-- COMMAND ----------

DESC DETAIL employees

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees
