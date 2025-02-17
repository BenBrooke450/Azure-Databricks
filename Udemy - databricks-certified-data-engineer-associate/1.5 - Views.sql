-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones
  (id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, "iPhone 14", "Apple"),
          (2, "iPhone 13", "apple"),
          (3,"iphone 6", "apple"),
          (4, "iPad Air","apple")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS SELECT * FROM smartphones WHERE brand = "apple";

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_apple_phones
AS SELECT * FROM smartphones WHERE brand = "apple";

-- COMMAND ----------

SELECT * FROM temp_view_apple_phones;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW Global_temp_view_apple_phones

 AS SELECT * FROM smartphones;

-- COMMAND ----------

SELECT * FROM global_temp.Global_temp_view_apple_phones

-- COMMAND ----------

SHOW TABLES IN global_temp
