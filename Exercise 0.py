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
# MAGIC ## Create Catalog and Schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS my_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

# COMMAND ----------

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
# MAGIC ## PySpark SQL

# COMMAND ----------

# Creating a Temp View for demo purposes
df.createOrReplaceTempView("people")

# COMMAND ----------

# Run a SQL query on the DataFrame
spark.sql("SELECT * FROM people").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering

# COMMAND ----------

# Filter people who are older than 40
df.filter(df["age"] > 40).display()

# COMMAND ----------

# Filter people who are older than 40 using SQL
spark.sql("SELECT * FROM people WHERE age > 40").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC

# COMMAND ----------

# Below transaction_amount is cast to double - for reference only
df_trans = df_trans.withColumn("transaction_amount", F.col("transaction_amount").cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting

# COMMAND ----------

# Sort by age in descending order
df.sort(df["age"].desc()).display()

# COMMAND ----------

# Sort by age in descending order using SQL
spark.sql("SELECT * FROM people ORDER BY age DESC").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining

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

# COMMAND ----------

# Sample 50% of the data
df.sample(False, 0.5).display()

# COMMAND ----------

# Sample 50% of the data using SQL
spark.sql("SELECT * FROM people TABLESAMPLE(50 PERCENT)").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

# Calculate summary statistics
df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Delta Table

# COMMAND ----------

df.write.saveAsTable("<catalog>.<schema>.<table>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Files

# COMMAND ----------

# Write the DataFrame to a CSV file
# Note that below path are pointing to DBFS (Databricks internal storage) and is not adviced to use in real life use cases.
df.write.csv("/tmp/people.csv")

# COMMAND ----------

# Write the DataFrame to a Parquet file
# Note that below path are pointing to DBFS (Databricks internal storage) and is not adviced to use in real life use cases.
df.write.parquet("/tmp/people.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data
# MAGIC

# COMMAND ----------

# Read batch
spark.read.csv("/tmp/people.csv").display()

# COMMAND ----------

# Read stream
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df_stream = spark.readStream.format("csv").schema(schema).load("/tmp/people.csv")
display(df_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write stream to delta table

# COMMAND ----------

# For reference only - Doesn't work
df_trans = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            #.option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/tmp/schema") # Schema location where the schema is stored
            .load(f"{w.external_locations.get('emhollanding').url}bootcamp/iot_stream/")
            )

(df_trans.writeStream
 .option("checkpointLocation", "/tmp/iot_stream_checkpoint") # Checkpoint location where the stream is checkpointed
 .option("partitionBy", "transaction_date")
 .trigger(once=True)
 .table("catalog.bronze.iot_stream")
)

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

# MAGIC %md
# MAGIC ## Clean up temp

# COMMAND ----------

dbutils.fs.ls("/tmp/")

# COMMAND ----------

dbutils.fs.rm("/tmp/people.parquet", True)
dbutils.fs.rm("/tmp/people.csv", True)
