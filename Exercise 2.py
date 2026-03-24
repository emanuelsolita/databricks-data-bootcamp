# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2: Data Transformation (Silver Layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC Transform and clean the Bronze layer data to create the Silver layer with proper data types, deduplication, and validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this exercise, you'll:
# MAGIC 1. Read data from Bronze tables
# MAGIC 2. Inspect for issues (wrong types, duplicates)
# MAGIC 3. Apply transformations
# MAGIC 4. Write clean data to Silver tables
# MAGIC
# MAGIC All output should go to: `<catalog>.silver.<table_name>`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Inspect Bronze Tables
# MAGIC
# MAGIC First, explore the Bronze tables to understand the data:
# MAGIC
# MAGIC 1. Read each Bronze table
# MAGIC 2. Check the schema (data types)
# MAGIC 3. Look for issues:
# MAGIC    - Wrong data types (e.g., strings instead of numbers)
# MAGIC    - Duplicate records
# MAGIC    - Null values
# MAGIC    - Date format issues
# MAGIC
# MAGIC Reference: `spark.read.table()`, `df.printSchema()`, `df.display()`, `df.groupBy().count()`

# COMMAND ----------


from pyspark.sql import functions as F

# COMMAND ----------


# TODO: Read and inspect bronze.iot_stream
df_trans = spark.read.table("emanuel_db.bronze.iot_stream")
df_trans.printSchema()
df_trans.display()

# COMMAND ----------


# TODO: Read and inspect bronze.customers
df_customers = spark.read.table("emanuel_db.bronze.customers")
df_customers.printSchema()
df_customers.display()

# COMMAND ----------


# TODO: Read and inspect bronze.products
df_products = spark.read.table("emanuel_db.bronze.products")
df_products.printSchema()
df_products.display()

# COMMAND ----------


# TODO: Read and inspect bronze.stores
df_stores = spark.read.table("emanuel_db.bronze.stores")
df_stores.printSchema()
df_stores.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Check for Duplicates
# MAGIC
# MAGIC For each table, check for duplicate IDs:
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy("id").count().filter("count > 1").display()
# MAGIC ```

# COMMAND ----------


# TODO: Check for duplicates in each table
df_customers.groupBy("id").count().filter("count > 1").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Transform IoT Stream
# MAGIC
# MAGIC Apply the following transformations to `iot_stream`:
# MAGIC
# MAGIC | Column | Cast To |
# MAGIC |--------|---------|
# MAGIC | transaction_amount | double |
# MAGIC | transaction_date | date |
# MAGIC | customer_id | long |
# MAGIC | product_id | long |
# MAGIC | store_id | long |
# MAGIC | receipt_id | long |
# MAGIC
# MAGIC Also:
# MAGIC - Drop the `_rescued_data` column if it exists
# MAGIC - Write to `emanuel_db.silver.iot_stream`
# MAGIC - Use streaming write with checkpoint
# MAGIC
# MAGIC Reference: `withColumn()`, `cast()`, `drop()`, `writeStream`

# COMMAND ----------


# TODO: Read and transform iot_stream
df_trans = (spark.readStream
             .table("emanuel_db.bronze.iot_stream"))

# COMMAND ----------


# TODO: Cast columns to correct types
df_trans = (df_trans
    .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
    .withColumn("transaction_date", F.to_date("transaction_date"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("product_id", F.col("product_id").cast("long"))
    .withColumn("store_id", F.col("store_id").cast("long"))
    .withColumn("receipt_id", F.col("receipt_id").cast("long"))
    .drop("_rescued_data")
)

# COMMAND ----------


# TODO: Write to silver with streaming
(df_trans.writeStream
    .format("delta")
    .option("checkpointLocation", "/usr/local/checkpoint/iot_stream")
    .option("overwriteSchema", "true")
    .trigger(once=True)
    .table("emanuel_db.silver.iot_stream"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Transform Customers
# MAGIC
# MAGIC Apply transformations to `customers`:
# MAGIC
# MAGIC Requirements:
# MAGIC - Remove duplicate records (keep one per `id`)
# MAGIC - Cast `date` column to proper date type
# MAGIC - Write to `emanuel_db.silver.customers`
# MAGIC - Use batch write
# MAGIC
# MAGIC Reference: `dropDuplicates()`, `to_date()`

# COMMAND ----------


# TODO: Deduplicate and transform customers
df_customers = spark.read.table("emanuel_db.bronze.customers")
df_customers = df_customers.dropDuplicates(["id"]).withColumn("date", F.to_date("date"))

# COMMAND ----------


# TODO: Write to silver
(df_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("emanuel_db.silver.customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Transform Products
# MAGIC
# MAGIC Apply the same pattern to `products`:
# MAGIC
# MAGIC Requirements:
# MAGIC - Remove duplicate records (keep one per `id`)
# MAGIC - Cast `date` column to proper date type
# MAGIC - Write to `emanuel_db.silver.products`

# COMMAND ----------


# TODO: Deduplicate and transform products
df_products = spark.read.table("emanuel_db.bronze.products")
df_products = df_products.dropDuplicates(["id"]).withColumn("date", F.to_date("date"))

# COMMAND ----------


# TODO: Write to silver
(df_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("emanuel_db.silver.products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Transform Stores
# MAGIC
# MAGIC Apply the same pattern to `stores`:
# MAGIC
# MAGIC Requirements:
# MAGIC - Remove duplicate records (keep one per `id`)
# MAGIC - Cast `date` column to proper date type
# MAGIC - Write to `emanuel_db.silver.stores`

# COMMAND ----------


# TODO: Deduplicate and transform stores
df_stores = spark.read.table("emanuel_db.bronze.stores")
df_stores = df_stores.dropDuplicates(["id"]).withColumn("date", F.to_date("date"))

# COMMAND ----------


# TODO: Write to silver
(df_stores.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("emanuel_db.silver.stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Verify Silver Tables
# MAGIC
# MAGIC Check your Silver layer:
# MAGIC 1. Verify schemas are correct
# MAGIC 2. Confirm no duplicates remain
# MAGIC 3. Count records and compare with Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Check silver tables
# MAGIC SHOW TABLES IN emanuel_db.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Count records in silver vs bronze
# MAGIC SELECT 'bronze' as layer, 'iot_stream' as table_name, COUNT(*) as cnt FROM emanuel_db.bronze.iot_stream
# MAGIC UNION ALL SELECT 'silver', 'iot_stream', COUNT(*) FROM emanuel_db.silver.iot_stream;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Verify no duplicates in silver tables
# MAGIC SELECT COUNT(*) - COUNT(DISTINCT id) as duplicate_count FROM emanuel_db.silver.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC ```python
# MAGIC dbutils.fs.rm("/usr/local/checkpoint/iot_stream", recurse=True)
# MAGIC spark.sql("DROP TABLE IF EXISTS emanuel_db.silver.iot_stream")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] Bronze tables inspected
# MAGIC - [ ] Issues identified (types, duplicates)
# MAGIC - [ ] `silver.iot_stream` created with correct types
# MAGIC - [ ] `silver.customers` created, deduplicated
# MAGIC - [ ] `silver.products` created, deduplicated
# MAGIC - [ ] `silver.stores` created, deduplicated
# MAGIC - [ ] Silver tables verified
