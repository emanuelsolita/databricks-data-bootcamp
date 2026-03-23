# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation with DLT (Silver Layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this exercise, we apply transformations to the bronze layer tables to create clean, validated silver layer tables.
# MAGIC
# MAGIC With DLT, transformations are simply defined as new tables that read from the bronze layer using the `live.` prefix.

# COMMAND ----------

# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform IoT Stream
# MAGIC Cast data types and clean the transaction data

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="iot_stream",
# MAGIC     comment="Cleaned and typed transaction stream"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction_amount", "transaction_amount IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
# MAGIC def clean_iot_stream():
# MAGIC     return (
# MAGIC         dlt.read_stream("iot_stream")  # Note: read_stream for streaming sources
# MAGIC         .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
# MAGIC         .withColumn("transaction_date", F.to_date("transaction_date"))
# MAGIC         .withColumn("customer_id", F.col("customer_id").cast("long"))
# MAGIC         .withColumn("product_id", F.col("product_id").cast("long"))
# MAGIC         .withColumn("store_id", F.col("store_id").cast("long"))
# MAGIC         .withColumn("receipt_id", F.col("receipt_id").cast("long"))
# MAGIC         .drop("_rescued_data", "id")  # Add a new unique ID
# MAGIC         .withColumn("id", F.expr("uuid()"))  # Generate unique ID
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication with Change Data Capture
# MAGIC
# MAGIC For dimensional data (customers, products, stores), we want to keep only the latest record (SCD Type 2 or just latest snapshot).

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="customers",
# MAGIC     comment="Deduplicated and typed customer data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def clean_customers():
# MAGIC     return (
# MAGIC         dlt.read("bronze.customers")  # Note: dlt.read for batch sources
# MAGIC         .dropDuplicates(["id"])
# MAGIC         .withColumn("date", F.to_date("date"))
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="products",
# MAGIC     comment="Deduplicated and typed product data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def clean_products():
# MAGIC     return (
# MAGIC         dlt.read("bronze.products")
# MAGIC         .dropDuplicates(["id"])
# MAGIC         .withColumn("date", F.to_date("date"))
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="stores",
# MAGIC     comment="Deduplicated and typed store data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def clean_stores():
# MAGIC     return (
# MAGIC         dlt.read("bronze.stores")
# MAGIC         .dropDuplicates(["id"])
# MAGIC         .withColumn("date", F.to_date("date"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL DLT Equivalent

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IoT Stream transformation (for batch/testing)
# MAGIC CREATE OR REFRESH LIVE TABLE iot_stream
# MAGIC AS SELECT
# MAGIC     CAST(transaction_amount AS DOUBLE) AS transaction_amount,
# MAGIC     CAST(transaction_date AS DATE) AS transaction_date,
# MAGIC     CAST(customer_id AS BIGINT) AS customer_id,
# MAGIC     CAST(product_id AS BIGINT) AS product_id,
# MAGIC     CAST(store_id AS BIGINT) AS store_id,
# MAGIC     CAST(receipt_id AS BIGINT) AS receipt_id,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM live.iot_stream;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers deduplication
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC AS SELECT * EXCEPT (etl_timestamp)
# MAGIC FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY etl_timestamp DESC) as rn
# MAGIC     FROM live.bronze_customers
# MAGIC )
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Expectations
# MAGIC
# MAGIC Add business rules to enforce data quality:

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="customers_clean")
# MAGIC @dlt.expect("valid_email_format", "customer_email LIKE '%@%' OR customer_email IS NULL")
# MAGIC @dlt.expect("reasonable_age", "customer_age > 0 AND customer_age < 150")
# MAGIC def customers_with_quality_rules():
# MAGIC     return dlt.read("silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Data Lineage
# MAGIC
# MAGIC DLT automatically tracks data lineage. After running the pipeline, you can see:
# MAGIC - Which tables feed into which
# MAGIC - Data quality metrics per table
# MAGIC - Schema evolution history
