-- Databricks notebook source
SELECT current_catalog();

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS managed_table 
  (width INT, length INT, height INT);

INSERT INTO managed_table 
    VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED managed_table; 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS New_database
LOCATION 'databricks_ws/New_database'

  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC clientID = dbutils.secrets.get(scope="formula1-scope", key="client-ID")
-- MAGIC tenantIDformula = dbutils.secrets.get(scope="formula1-scope", key="tenant-ID-formula")
-- MAGIC Clientsecret = dbutils.secrets.get(scope="formula1-scope", key="Client-secret")
-- MAGIC
-- MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
-- MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
-- MAGIC           "fs.azure.account.oauth2.client.id": clientID,
-- MAGIC           "fs.azure.account.oauth2.client.secret": Clientsecret,
-- MAGIC           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantIDformula}/oauth2/token"}
-- MAGIC
-- MAGIC
-- MAGIC dbutils.fs.mount(source="abfss://tester@formula1dlben.dfs.core.windows.net/",
-- MAGIC                   mount_point="/mnt/formula1dlben/tester",
-- MAGIC                   extra_configs= configs)
-- MAGIC
-- MAGIC """
-- MAGIC abfss://presentation@formula1dlben.dfs.core.windows.net/
-- MAGIC abfss://demo@formula1dlben.dfs.core.windows.net/
-- MAGIC abfss://raw@formula1dlben.dfs.core.windows.net/
-- MAGIC abfss://processed@formula1dlben.dfs.core.windows.net/
-- MAGIC
-- MAGIC
-- MAGIC /mnt/formula1dlben/processed
-- MAGIC /mnt/formula1dlben/raw
-- MAGIC /mnt/formula1dlben/presentation
-- MAGIC /mnt/formula1dlben/demo
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/mnt/formula1dlben/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.refreshMounts()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/mnt"))

-- COMMAND ----------

USE new_database;

CREATE TABLE IF NOT EXISTS managed_table_database
  (width INT, length INT, height INT);

INSERT INTO  managed_table_database
    VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

USE hive_metastore.default;

CREATE TABLE IF NOT EXISTS managed_table_database
  (width INT, length INT, height INT);

INSERT INTO  managed_table_database
    VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE DETAIL hive_metastore.default.managed_table_database

-- COMMAND ----------

describe detail managed_table_database
