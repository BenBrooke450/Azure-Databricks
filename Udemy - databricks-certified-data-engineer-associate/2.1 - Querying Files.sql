-- Databricks notebook source
-- MAGIC %run ./Setup_and_Data/Copy-Datasets
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/mnt/"))

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT COUNT(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT *, input_file_name() AS source_file
  FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

USE databricks_ws.new_database;

DROP TABLE IF EXISTS managed_table_database

-- COMMAND ----------

USE hive_metastore.default;

CREATE DATABASE Databrick_DE

-- COMMAND ----------


USE hive_metastore.Databrick_DE;

CREATE TABLE book_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS ( header = "true",
          delimiter = ";")
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM book_csv

-- COMMAND ----------

DESC EXTENDED book_csv

-- COMMAND ----------

SELECT count(*) FROM book_csv

-- COMMAND ----------

USE hive_metastore.Databrick_DE;

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

DESC customers 

-- COMMAND ----------

DESCRIBE EXTENDED customers
