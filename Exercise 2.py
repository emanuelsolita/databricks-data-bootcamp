# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation 

# COMMAND ----------

# MAGIC %md
# MAGIC In this Exercise we explore the data we wrote to our raw layer (bronze) a little further. We look out for duplicates, data types and other formatting issues that the raw data might experience.
# MAGIC
# MAGIC The final ouput of this exercise will be the final datasets that does not contain any duplicate values and has proper data types. These should then be written to the ```silver``` schema in your Catalog. 
# MAGIC
# MAGIC Please use **Exercise 0** as reference for different transformations

# COMMAND ----------

# Read the tables to inspect the data (e.g. summarize to view the data composition) and its metadata (e.g. data types)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read transaction stream and write it to silver
# MAGIC For the transaction table, inspect the data, make sure there are no duplicates and that the data types are ok.
# MAGIC
# MAGIC If not, remove duplicates, transform the data types.
# MAGIC - Group by id 
# MAGIC - Transformations on columns - df.withColumn()
# MAGIC
# MAGIC
# MAGIC Import pyspark functions.
# MAGIC `from pyspark.sql import functions as F`
# MAGIC
# MAGIC Write stream to table with output mode append and trigger once and write to silver.
# MAGIC
# MAGIC https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options 

# COMMAND ----------

from pyspark.sql import functions as F



# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and explore Customers, Products and Stores
# MAGIC Some things to look for
# MAGIC - Appropriate data types
# MAGIC - Duplicates
# MAGIC
# MAGIC
# MAGIC Make appropriate changes and write to silver schema. 
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


