# Databricks notebook source
# MAGIC %md
# MAGIC # Getting started

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Cluster
# MAGIC If not done already, please navigate to the cluster tab in the left pane. Under all-purpose compute, create a Personal compute as pictured below
# MAGIC
# MAGIC ![create compute](./docs/create_compute.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore PySpark

# COMMAND ----------

# Create a customer table with 5 customers
data = [(1, 'Alice', 'Smith', 25, 'Female'),
        (2, 'Bob', 'Johnson', 30, 'Male'),
        (3, 'Charlie', 'Brown', 28, 'Male'),
        (4, 'Diana', 'Williams', 35, 'Female'),
        (5, 'Eve', 'Davis', 40, 'Female')]

columns = ['customer_id', 'first_name', 'last_name', 'age', 'gender']

customer_df = spark.createDataFrame(data, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema
# MAGIC
