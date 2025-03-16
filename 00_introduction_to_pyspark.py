# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to PySpark
# MAGIC
# MAGIC This notebook provides an introduction to using PySpark in Databricks. It covers the following topics:
# MAGIC
# MAGIC - What is PySpark?
# MAGIC - Why use PySpark?
# MAGIC - PySpark Data Structures
# MAGIC - PySpark DataFrame Operations
# MAGIC - PySpark SQL
# MAGIC - PySpark MLlib
# MAGIC
# MAGIC PySpark is the Python API for Apache Spark, which is an open-source, distributed computing system used for big data processing and analytics. PySpark provides an easy-to-use programming abstraction and parallel runtime that allows you to do big data processing with Python.
# MAGIC
# MAGIC This notebook is based on the [PySpark documentation](https://spark.apache.org/docs/latest/api/python/index.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations and Actions
# MAGIC
# MAGIC PySpark operations can be divided into two categories: transformations and actions.
# MAGIC
# MAGIC - **Transformations** are operations that transform an RDD into another RDD. Examples include `map`, `filter`, and `reduceByKey`.
# MAGIC - **Actions** are operations that trigger computation and return results. Examples include `count`, `collect`, and `saveAsTextFile`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## PySpark Data Structures
# MAGIC
# MAGIC PySpark provides two main data structures: RDDs and DataFrames.
# MAGIC
# MAGIC - **Resilient Distributed Datasets (RDDs)** are the building blocks of PySpark. They are immutable distributed collections of objects. Each RDD is split into multiple partitions, which may be computed on different nodes of the cluster.
# MAGIC - **DataFrames** are distributed collections of data organized into named columns. They are conceptually equivalent to tables in a relational database or data frames in R/Python, but with richer optimizations under the hood.

# COMMAND ----------
# MAGIC %md
# MAGIC ## PySpark DataFrame Operations
# MAGIC
# MAGIC PySpark DataFrames support a wide range of operations, including the following:
# MAGIC
# MAGIC - **Aggregations**: `groupBy`, `avg`, `max`, `min`, etc.
# MAGIC - **Filtering**: `filter`, `where`
# MAGIC - **Sorting**: `sort`, `orderBy`
# MAGIC - **Joining**: `join`, `crossJoin`
# MAGIC - **Sampling**: `sampleBy`, `randomSplit`
# MAGIC - **Statistics**: `describe`
# MAGIC - **Writing to Files**: `write`, `saveAsTable`
# MAGIC
# MAGIC PySpark DataFrames can be created from a variety of data sources, including CSV, JSON, Avro, Parquet, ORC, JDBC, and more.

# COMMAND ----------
# MAGIC %md
# MAGIC ## PySpark SQL
# MAGIC
# MAGIC PySpark SQL provides a domain-specific language for working with structured data. It includes the following features:
# MAGIC
# MAGIC - **SQL Queries**: Run SQL queries on DataFrames.
# MAGIC - **User-Defined Functions (UDFs)**: Define custom functions in Python and use them in SQL queries.
# MAGIC - **Hive Integration**: Access Hive tables and run Hive queries.
# MAGIC
# MAGIC PySpark SQL is used to process structured data, such as tables in a relational database or data frames in R/Python.

# COMMAND ----------
# MAGIC %md
# MAGIC ## PySpark MLlib
# MAGIC
# MAGIC PySpark MLlib is the machine learning library in PySpark. It provides a wide range of machine learning algorithms and utilities, including the following:
# MAGIC
# MAGIC - **Classification**: Logistic Regression, Decision Trees, Random Forest, Gradient Boosting, etc.
# MAGIC - **Regression**: Linear Regression, Generalized Linear Regression, Decision Trees, Random Forest, Gradient Boosting, etc.
# MAGIC - **Clustering**: K-Means, Gaussian Mixture, etc.
# MAGIC - **Recommendation**: Alternating Least Squares (ALS)
# MAGIC - **Dimensionality Reduction**: Principal Component Analysis (PCA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations
# MAGIC
# COMMAND ----------

# Create a DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 56)]
df = spark.createDataFrame(data, ["name", "age"])
df.display()

# COMMAND ----------
# Group by age and count the number of people in each age group
df.groupBy("age").count().display()

# COMMAND ----------
# Calculate the average age
df.agg({"age": "avg"}).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering
# MAGIC
# COMMAND ----------
# Filter people who are older than 40
df.filter(df["age"] > 40).display()

# COMMAND ----------
# Filter people who are older than 40 using SQL

df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE age > 40").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sorting
# MAGIC
# COMMAND ----------
# Sort by age in descending order
df.sort(df["age"].desc()).display()

# COMMAND ----------
# Sort by age in descending order using SQL
spark.sql("SELECT * FROM people ORDER BY age DESC").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Joining
# MAGIC
# COMMAND ----------
# Create two DataFrames
data1 = [("Alice", 34), ("Bob", 45), ("Charlie", 56)]
df1 = spark.createDataFrame(data1, ["name", "age"])
data2 = [("Alice", "Engineer"), ("Bob", "Doctor"), ("Charlie", "Lawyer")]
df2 = spark.createDataFrame(data2, ["name", "profession"])

# Join the two DataFrames on the "name" column
df1.join(df2, "name").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Joining on different columns
# COMMAND ----------
# Create two DataFrames
data1 = [("Alice", 34), ("Bob", 45), ("Charlie", 56)]
df1 = spark.createDataFrame(data1, ["name", "age"])
data2 = [("Alice", "Engineer"), ("Bob", "Doctor"), ("Charlie", "Lawyer")]
df2 = spark.createDataFrame(data2, ["person", "profession"])

# Join the two DataFrames on different columns
df1.join(df2, df1["name"] == df2["person"]).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sampling
# MAGIC
# COMMAND ----------
# Sample 50% of the data
df.sample(False, 0.5).display()

# COMMAND ----------
# Sample 50% of the data using SQL
spark.sql("SELECT * FROM people TABLESAMPLE(50 PERCENT)").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Statistics
# MAGIC
# COMMAND ----------
# Calculate summary statistics
df.describe().display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Writing to Files
# COMMAND ----------
# Write the DataFrame to a CSV file
df.write.csv("/tmp/people.csv")

# COMMAND ----------
# Write the DataFrame to a Parquet file
df.write.parquet("/tmp/people.parquet")

# COMMAND ----------
# MAGIC %md
# MAGIC ## PySpark SQL
# MAGIC
# COMMAND ----------
# Create a DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 56)]
df = spark.createDataFrame(data, ["name", "age"])
df.createOrReplaceTempView("people")

# COMMAND ----------
# Run a SQL query on the DataFrame
spark.sql("SELECT * FROM people").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## User-Defined Functions (UDFs)
# COMMAND ----------
# Define a UDF that adds 10 to a number
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

add_10 = udf(lambda x: x + 10, IntegerType())

# Apply the UDF to the "age" column
df.withColumn("age_plus_10", add_10(df["age"])).display()

# COMMAND ----------