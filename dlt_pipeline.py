# Databricks notebook source
# MAGIC %md
# MAGIC # Complete DLT Pipeline - Retail Analytics
# MAGIC
# MAGIC This notebook implements a complete Delta Live Tables pipeline with:
# MAGIC - **Bronze Schema**: Raw data ingestion from landing zone
# MAGIC - **Silver Schema**: Data cleansing and transformations
# MAGIC - **Gold Schema**: Dimension and fact tables for analytics

# COMMAND ----------

# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE SCHEMA - Data Ingestion
# MAGIC
# MAGIC Ingests raw data from the landing zone with data quality expectations.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.bronze.iot_stream_dlt",
# MAGIC     comment="Raw IoT transaction stream from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction_id", "id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
# MAGIC def bronze_iot_stream_dlt():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.bronze.customers_dlt",
# MAGIC     comment="Raw customer data from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_customer_id", "id IS NOT NULL")
# MAGIC def bronze_customers_dlt():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.bronze.products_dlt",
# MAGIC     comment="Raw product data from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_product_id", "id IS NOT NULL")
# MAGIC def bronze_products_dlt():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.bronze.stores_dlt",
# MAGIC     comment="Raw store data from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_store_id", "id IS NOT NULL")
# MAGIC def bronze_stores_dlt():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER SCHEMA - Data Transformation
# MAGIC
# MAGIC Applies data cleansing, type casting, and deduplication.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.silver.iot_stream_dlt",
# MAGIC     comment="Cleaned and typed transaction stream"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction_amount", "transaction_amount IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
# MAGIC def silver_iot_stream_dlt():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze.iot_stream_dlt")
# MAGIC         .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
# MAGIC         .withColumn("transaction_date", F.to_date("transaction_date"))
# MAGIC         .withColumn("customer_id", F.col("customer_id").cast("long"))
# MAGIC         .withColumn("product_id", F.col("product_id").cast("long"))
# MAGIC         .withColumn("store_id", F.col("store_id").cast("long"))
# MAGIC         .withColumn("receipt_id", F.col("receipt_id").cast("long"))
# MAGIC         .drop("_rescued_data")
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.silver.customers_dlt",
# MAGIC     comment="Deduplicated and typed customer data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC @dlt.expect("valid_email_format", "customer_email LIKE '%@%' OR customer_email IS NULL")
# MAGIC @dlt.expect("reasonable_age", "customer_age > 0 AND customer_age < 150")
# MAGIC def silver_customers_dlt():
# MAGIC     return (
# MAGIC         dlt.read("bronze.customers_dlt")
# MAGIC         .dropDuplicates(["id"])
# MAGIC         .withColumn("date", F.to_date("date"))
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.silver.products_dlt",
# MAGIC     comment="Deduplicated and typed product data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def silver_products_dlt():
# MAGIC     return (
# MAGIC         dlt.read("bronze.products_dlt")
# MAGIC         .dropDuplicates(["id"])
# MAGIC         .withColumn("date", F.to_date("date"))
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.silver.stores_dlt",
# MAGIC     comment="Deduplicated and typed store data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def silver_stores_dlt():
# MAGIC     return (
# MAGIC         dlt.read("bronze.stores_dlt")
# MAGIC         .dropDuplicates(["id"])
# MAGIC         .withColumn("date", F.to_date("date"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD SCHEMA - Dimension and Fact Tables
# MAGIC
# MAGIC Creates analytics-ready dimension and fact tables.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.gold.dim_customers_dlt",
# MAGIC     comment="Customer dimension table"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
# MAGIC def gold_dim_customers_dlt():
# MAGIC     return (
# MAGIC         dlt.read("silver.customers_dlt")
# MAGIC         .select(
# MAGIC             "id",
# MAGIC             "customer_name",
# MAGIC             "customer_email",
# MAGIC             "customer_age",
# MAGIC             "etl_timestamp"
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.gold.dim_stores_dlt",
# MAGIC     comment="Store dimension table"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
# MAGIC def gold_dim_stores_dlt():
# MAGIC     return (
# MAGIC         dlt.read("silver.stores_dlt")
# MAGIC         .select(
# MAGIC             "id",
# MAGIC             "store_name",
# MAGIC             "store_city",
# MAGIC             "store_country",
# MAGIC             "etl_timestamp"
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.gold.dim_products_dlt",
# MAGIC     comment="Product dimension table"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
# MAGIC def gold_dim_products_dlt():
# MAGIC     return (
# MAGIC         dlt.read("silver.products_dlt")
# MAGIC         .select(
# MAGIC             "id",
# MAGIC             "product_name",
# MAGIC             "product_category",
# MAGIC             "product_price",
# MAGIC             "etl_timestamp"
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.gold.fact_iot_transactions_dlt",
# MAGIC     comment="IoT transaction fact table with dimension joins"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction", "transaction_amount IS NOT NULL AND transaction_date IS NOT NULL")
# MAGIC def gold_fact_iot_transactions_dlt():
# MAGIC     iot = dlt.read_stream("silver.iot_stream_dlt")
# MAGIC     customers = dlt.read("silver.customers_dlt")
# MAGIC     products = dlt.read("silver.products_dlt")
# MAGIC     stores = dlt.read("silver.stores_dlt")

# MAGIC     return (
# MAGIC         iot
# MAGIC         .join(customers, iot.customer_id == customers.id, "left")
# MAGIC         .join(products, iot.product_id == products.id, "left")
# MAGIC         .join(stores, iot.store_id == stores.id, "left")
# MAGIC         .select(
# MAGIC             iot.id,
# MAGIC             iot.transaction_date,
# MAGIC             iot.transaction_amount,
# MAGIC             iot.customer_id,
# MAGIC             iot.product_id,
# MAGIC             iot.store_id,
# MAGIC             iot.etl_timestamp,
# MAGIC             customers.customer_name,
# MAGIC             products.product_name,
# MAGIC             products.product_category,
# MAGIC             stores.store_name,
# MAGIC             stores.store_city
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="emanuel_db.gold.sales_daily_summary_dlt",
# MAGIC     comment="Daily sales summary for dashboards"
# MAGIC )
# MAGIC def gold_sales_daily_summary_dlt():
# MAGIC     return (
# MAGIC         dlt.read("gold.fact_iot_transactions_dlt")
# MAGIC         .groupBy("transaction_date", "store_name", "product_category")
# MAGIC         .agg(
# MAGIC             F.count("*").alias("transaction_count"),
# MAGIC             F.sum("transaction_amount").alias("total_revenue"),
# MAGIC             F.avg("transaction_amount").alias("avg_transaction")
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary
# MAGIC
# MAGIC | Schema | Table Name | Description |
# MAGIC |--------|------------|-------------|
# MAGIC | bronze | iot_stream_dlt | Raw IoT transaction stream |
# MAGIC | bronze | customers_dlt | Raw customer data |
# MAGIC | bronze | products_dlt | Raw product data |
# MAGIC | bronze | stores_dlt | Raw store data |
# MAGIC | silver | iot_stream_dlt | Cleaned transaction stream |
# MAGIC | silver | customers_dlt | Deduplicated customers |
# MAGIC | silver | products_dlt | Deduplicated products |
# MAGIC | silver | stores_dlt | Deduplicated stores |
# MAGIC | gold | dim_customers_dlt | Customer dimension |
# MAGIC | gold | dim_stores_dlt | Store dimension |
# MAGIC | gold | dim_products_dlt | Product dimension |
# MAGIC | gold | fact_iot_transactions_dlt | Transaction fact table |
# MAGIC | gold | sales_daily_summary_dlt | Daily sales aggregation |

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Run This Pipeline
# MAGIC
# MAGIC 1. **Create Schemas** (if not exists):
# MAGIC    ```sql
# MAGIC    CREATE SCHEMA IF NOT EXISTS emanuel_db.bronze;
# MAGIC    CREATE SCHEMA IF NOT EXISTS emanuel_db.silver;
# MAGIC    CREATE SCHEMA IF NOT EXISTS emanuel_db.gold;
# MAGIC    ```
# MAGIC
# MAGIC 2. Go to **Delta Live Tables** in the sidebar
# MAGIC 3. Click **Create Pipeline**
# MAGIC 4. Add this notebook to the pipeline
# MAGIC 5. Configure settings:
# MAGIC    - **Pipeline Name**: `retail_analytics_dlt`
# MAGIC    - **Target Catalog**: `emanuel_db`
# MAGIC    - **Target Schema**: Leave empty (tables specify full path)
# MAGIC    - **Pipeline Mode**: `Continuous` or `Triggered`
# MAGIC 6. Add the external location path in pipeline settings
# MAGIC 7. Click **Create** and then **Start**
