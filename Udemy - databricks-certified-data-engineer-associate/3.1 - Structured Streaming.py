# Databricks notebook source
# MAGIC %run ./Setup_and_Data/Copy-Datasets

# COMMAND ----------

spark.sql('USE hive_metastore.databrick_DE')


(spark.readStream.
            table("book_csv").
            createOrReplaceTempView("books_streaming_temp_view"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_temp_view
# MAGIC   GROUP BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM books_streaming_temp_view
# MAGIC ORDER BY author 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_view AS
# MAGIC   (SELECT author, COUNT(book_id) AS total_books
# MAGIC   FROM books_streaming_temp_view
# MAGIC   GROUP BY author
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS

# COMMAND ----------

(spark.table("author_counts_tmp_view")                               
      .writeStream  
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO book_csv
# MAGIC   values ("B19","Introduction to Modeling","Mark W. Spong","Computer science",25)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO book_csv
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS
# MAGIC

# COMMAND ----------

(spark.table("author_counts_tmp_view")                               
      .writeStream  
      .trigger(availableNow = True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination())

# COMMAND ----------


