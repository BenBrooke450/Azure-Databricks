-- Databricks notebook source
-- MAGIC %run ./Setup_and_Data/Copy-Datasets

-- COMMAND ----------

USE hive_metastore.databrick_DE;

SELECT * FROM orders

-- COMMAND ----------

SELECT 
  order_id,
  books,
  FILTER (books, i-> i.quantity >= 2 ) AS multiple_copies
  FROM orders

-- COMMAND ----------

SELECT order_id, multiple_copies
 FROM (
  SELECT order_id, 
  FILTER (books, i-> i.quantity >= 2 ) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------


