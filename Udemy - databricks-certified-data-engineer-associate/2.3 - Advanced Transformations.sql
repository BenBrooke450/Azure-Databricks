-- Databricks notebook source
-- MAGIC %run ./Setup_and_Data/Copy-Datasets

-- COMMAND ----------

USE hive_metastore.databrick_de;

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, email FROM customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:last_name, email FROM customers

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers

  

-- COMMAND ----------

SELECT profile FROM customers LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
SELECT customer_id, from_json(profile,schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) AS profile_struct
  FROM customers;

SELECT * FROM parsed_customers;

-- COMMAND ----------

DESCRIBE parsed_customers;

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.last_name FROM parsed_customers;

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS books
  FROM orders

-- COMMAND ----------

SELECT customer_id, collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders 
 GROUP BY customer_id

-- COMMAND ----------

SELECT customer_id, collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS before_flattern,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flattern
FROM orders 
 GROUP BY customer_id
