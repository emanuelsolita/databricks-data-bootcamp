# Databricks notebook source
# MAGIC %md
# MAGIC # Complete DLT Pipeline - Retail Analytics
# MAGIC
# MAGIC This notebook implements a complete Delta Live Tables pipeline with:
# MAGIC - **Bronze Schema**: Raw data ingestion from landing zone
# MAGIC - **Silver Schema**: Data cleansing and transformations
# MAGIC - **Gold Schema**: Dimension and fact tables for analytics

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE SCHEMA - Data Ingestion
# MAGIC
# MAGIC Ingests raw data from the landing zone with data quality expectations.

# COMMAND ----------

@dp.table(
    name="emanuel_db.bronze.iot_stream_dlt",
    comment="Raw IoT transaction stream from landing zone"
)
@dp.expect_or_drop("valid_transaction_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
def bronze_iot_stream_dlt():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.bronze.customers_dlt",
    comment="Raw customer data from landing zone"
)
@dp.expect_or_drop("valid_customers_id", "id IS NOT NULL")
def bronze_customers_dlt():
    return (
        spark.read
        .format("json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.bronze.products_dlt",
    comment="Raw product data from landing zone"
)
@dp.expect_or_drop("valid_product_id", "id IS NOT NULL")
def bronze_products_dlt():
    return (
        spark.read
        .format("json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.bronze.stores_dlt",
    comment="Raw store data from landing zone"
)
@dp.expect_or_drop("valid_store_id", "id IS NOT NULL")
def bronze_stores_dlt():
    return (
        spark.read
        .format("json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER SCHEMA - Data Transformation
# MAGIC
# MAGIC Applies data cleansing, type casting, and deduplication.

# COMMAND ----------

@dp.table(
    name="emanuel_db.silver.iot_stream_dlt",
    comment="Cleaned and typed transaction stream"
)
@dp.expect_or_drop("valid_transaction_amount", "transaction_amount IS NOT NULL")
@dp.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
def silver_iot_stream_dlt():
    return (
        dp.read_stream("bronze.iot_stream_dlt")
        .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
        .withColumn("transaction_date", F.to_date("transaction_date"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("store_id", F.col("store_id").cast("long"))
        .withColumn("receipt_id", F.col("receipt_id").cast("long"))
        .drop("_rescued_data")
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.silver.customers_dlt",
    comment="Deduplicated and typed customer data"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
#@dp.expect("valid_email_format", "customer_email LIKE '%@%' OR customer_email IS NULL")
@dp.expect("reasonable_age", "age > 0 AND age < 150")
def silver_customers_dlt():
    return (
        dp.read("bronze.customers_dlt")
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("date"))
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.silver.products_dlt",
    comment="Deduplicated and typed product data"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_products_dlt():
    return (
        dp.read("bronze.products_dlt")
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("date"))
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.silver.stores_dlt",
    comment="Deduplicated and typed store data"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_stores_dlt():
    return (
        dp.read("bronze.stores_dlt")
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("date"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD SCHEMA - Dimension and Fact Tables
# MAGIC
# MAGIC Creates analytics-ready dimension and fact tables.

# COMMAND ----------

@dp.table(
    name="emanuel_db.gold.dim_customers_dlt",
    comment="Customer dimension table"
)
@dp.expect_or_drop("valid_pk", "id IS NOT NULL")
def gold_dim_customers_dlt():
    return (
        dp.read("silver.customers_dlt")
        .select(
            "id",
            "customer_name",
            "address",
            "age",
            "gender",
            "etl_timestamp"
        )
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.gold.dim_stores_dlt",
    comment="Store dimension table"
)
@dp.expect_or_drop("valid_pk", "id IS NOT NULL")
def gold_dim_stores_dlt():
    return (
        dp.read("silver.stores_dlt")
        .select(
            "id",
            "store_name",
            "city",
            "address",
            "etl_timestamp"
        )
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.gold.dim_products_dlt",
    comment="Product dimension table"
)
@dp.expect_or_drop("valid_pk", "id IS NOT NULL")
def gold_dim_products_dlt():
    return (
        dp.read("silver.products_dlt")
        .select(
            "id",
            "product_name",
            "product_category",
            "product_description",
            "etl_timestamp"
        )
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.gold.fact_iot_transactions_dlt",
    comment="IoT transaction fact table with dimension joins"
)
@dp.expect_or_drop("valid_transaction", "transaction_amount IS NOT NULL AND transaction_date IS NOT NULL")
def gold_fact_iot_transactions_dlt():
    iot = dp.read_stream("silver.iot_stream_dlt")
    customers = dp.read("silver.customers_dlt")
    products = dp.read("silver.products_dlt")
    stores = dp.read("silver.stores_dlt")

    return (
        iot
        .join(customers, iot.customer_id == customers.id, "left")
        .join(products, iot.product_id == products.id, "left")
        .join(stores, iot.store_id == stores.id, "left")
        .select(
            iot.id,
            iot.transaction_date,
            iot.transaction_amount,
            iot.customer_id,
            iot.product_id,
            iot.store_id,
            iot.etl_timestamp,
            customers.customer_name,
            products.product_name,
            products.product_category,
            stores.store_name,
            stores.city
        )
    )

# COMMAND ----------

@dp.table(
    name="emanuel_db.gold.sales_daily_summary_dlt",
    comment="Daily sales summary for dashboards"
)
def gold_sales_daily_summary_dlt():
    return (
        dp.read("gold.fact_iot_transactions_dlt")
        .groupBy("transaction_date", "store_name", "product_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("transaction_amount").alias("total_revenue"),
            F.avg("transaction_amount").alias("avg_transaction")
        )
    )

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
