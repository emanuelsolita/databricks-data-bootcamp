# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 0: Getting Started with PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC Get familiar with the Databricks environment and learn the basics of PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create a Cluster
# MAGIC If not already done, create a personal compute cluster:
# MAGIC 1. Navigate to the **Compute** tab in the left sidebar
# MAGIC 2. Under **All-purpose compute**, click **Create compute**
# MAGIC 3. Select appropriate runtime and configuration
# MAGIC
# MAGIC ![create compute](./docs/create_compute.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Create Catalog and Schemas
# MAGIC Create the necessary catalog and schemas for the medallion architecture:
# MAGIC
# MAGIC | Schema | Purpose |
# MAGIC |--------|---------|
# MAGIC | `bronze` | Raw, ingested data |
# MAGIC | `silver` | Cleaned, transformed data |
# MAGIC | `gold` | Aggregated, business-ready data |
# MAGIC
# MAGIC Use SQL to create:
# MAGIC 1. A **catalog** named `emanuel_db`
# MAGIC 2. Three **schemas**: `bronze`, `silver`, `gold` under the catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create the catalog
# MAGIC CREATE CATALOG <catalog_name>;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create the bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS <catalog_name>.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create the silver schema
# MAGIC CREATE SCHEMA IF NOT EXISTS <catalog_name>.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create the gold schema
# MAGIC CREATE SCHEMA IF NOT EXISTS <catalog_name>.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Fundamentals

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Create and Manipulate DataFrames
# MAGIC
# MAGIC Practice creating DataFrames and performing basic operations. Create a DataFrame with sample data containing:
# MAGIC - Two columns: `name` (string) and `age` (integer)
# MAGIC - At least 5 rows of data
# MAGIC
# MAGIC Reference: `spark.createDataFrame()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Create a DataFrame with sample data
# MAGIC data = [...]  # Define your data
# MAGIC df = ...      # Create the DataFrame
# MAGIC df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Aggregations
# MAGIC
# MAGIC Using the DataFrame you created:
# MAGIC 1. Count the number of records per category (e.g., by age)
# MAGIC 2. Calculate the average of a numeric column
# MAGIC
# MAGIC Reference: `groupBy()`, `count()`, `agg()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Count records by category
# MAGIC df.groupBy(...).count().display()

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Calculate average of numeric column
# MAGIC df.agg(...).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: SQL Queries
# MAGIC
# MAGIC Register your DataFrame as a temporary view and run SQL queries:
# MAGIC 1. Create a temp view from your DataFrame
# MAGIC 2. Run a SELECT query using `spark.sql()`
# MAGIC
# MAGIC Reference: `createOrReplaceTempView()`, `spark.sql()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Create temp view and run SQL query
# MAGIC df.createOrReplaceTempView("my_view")
# MAGIC spark.sql("SELECT * FROM my_view").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Filtering
# MAGIC
# MAGIC Filter data using both PySpark syntax and SQL:
# MAGIC 1. Filter rows where a numeric column meets a condition
# MAGIC 2. Replicate the same filter using SQL
# MAGIC
# MAGIC Reference: `filter()`, `where()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Filter using PySpark
# MAGIC df.filter(...).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Filter using SQL
# MAGIC SELECT * FROM my_view WHERE ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Sorting
# MAGIC
# MAGIC Sort your DataFrame by a column:
# MAGIC 1. Sort in ascending order
# MAGIC 2. Sort in descending order
# MAGIC
# MAGIC Reference: `sort()`, `orderBy()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Sort data
# MAGIC df.sort(...).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8: Joining
# MAGIC
# MAGIC Practice joining two DataFrames:
# MAGIC 1. Create a second DataFrame with additional information
# MAGIC 2. Join both DataFrames on a common column
# MAGIC 3. Join on different column names
# MAGIC
# MAGIC Reference: `join()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Create second DataFrame
# MAGIC data2 = [...]
# MAGIC df2 = ...

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Join on same column name
# MAGIC df.join(df2, "common_column").display()

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Join on different column names
# MAGIC df.join(df2, df["col1"] == df2["col2"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9: Writing to Tables
# MAGIC
# MAGIC Save your DataFrame to a Delta table:
# MAGIC 1. Write to a table using `saveAsTable()`
# MAGIC 2. Use the three-level naming: `<catalog>.<schema>.<table_name>`
# MAGIC
# MAGIC Reference: `write.saveAsTable()`

# COMMAND ----------

# MAGIC %python
# MAGIC # TODO: Write DataFrame to Delta table
# MAGIC df.write.saveAsTable("<catalog>.<schema>.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC Before moving on, verify you've completed:
# MAGIC - [ ] Cluster created
# MAGIC - [ ] Catalog and schemas created
# MAGIC - [ ] DataFrame operations (create, filter, sort, join)
# MAGIC - [ ] SQL queries on DataFrames
# MAGIC - [ ] Data written to Delta table
