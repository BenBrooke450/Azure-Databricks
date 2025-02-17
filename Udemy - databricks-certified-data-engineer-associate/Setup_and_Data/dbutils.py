# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/"))


# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/user/hive/warehouse/"))
