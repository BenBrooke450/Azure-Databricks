# Databricks notebook source
# MAGIC %run ./Setup_and_Data/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
# MAGIC COMMENT "The raw books orders, ingested from orders-raw"
# MAGIC AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
# MAGIC                              map("cloudFiles.inferColumnTypes", "true"))
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC COMMENT "The customers lookup table, ingested from customers-json"
# MAGIC AS SELECT * FROM json.`${datasets.path}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
# MAGIC   CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "The cleaned books orders with valid order_id"
# MAGIC AS
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
# MAGIC          c.profile:address:country as country
# MAGIC   FROM STREAM(LIVE.orders_raw) o
# MAGIC   LEFT JOIN LIVE.customers c
# MAGIC     ON o.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
# MAGIC COMMENT "Daily number of books per customer in China"
# MAGIC AS
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM LIVE.orders_cleaned
# MAGIC   WHERE country = "China"
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC

# COMMAND ----------



"""
You are given an integer array nums.
    The unique elements of an array are the elements that appear exactly once in the array.

Return the sum of all the unique elements of nums.



Example 1:

Input: nums = [1,2,3,2]
Output: 4
Explanation: The unique elements are [1,3], and the sum is 4.
Example 2:

Input: nums = [1,1,1,1,1]
Output: 0
Explanation: There are no unique elements, and the sum is 0.
Example 3:

Input: nums = [1,2,3,4,5]
Output: 15
Explanation: The unique elements are [1,2,3,4,5], and the sum is 15.

"""

import numpy as np


def sumOfUnique(nums: list[int]) -> int:
    return sum([x for x in nums if nums.count(x) == 1])


print(sumOfUnique([1,2,3,4,5]))
#15

print(sumOfUnique([1,2,3,2]))
#4





#####################################################


import numpy as np


def sumOfUnique(nums: list[int]) -> int:
    return sum((np.unique(nums)).tolist())


print(sumOfUnique([1,2,3,4,5]))
#15

print(sumOfUnique([1,2,3,2]))
#6



