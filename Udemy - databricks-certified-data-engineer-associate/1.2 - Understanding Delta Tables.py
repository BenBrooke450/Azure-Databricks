# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employees
# MAGIC  (id INT, name STRING, salary DOUBLE)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES 
# MAGIC   (1, "Adam", 3500.0),
# MAGIC   (2, "Sarah", 4020.5),
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3),
# MAGIC   (5, "Anna", 2500.0),
# MAGIC   (6, "Kim", 6200.3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_METASTORE();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_ws.default.employees

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/"))


# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees
# MAGIC SET salary = salary * 100
# MAGIC WHERE name LIKE 'A%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees
