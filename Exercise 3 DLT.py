# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregation with DLT (Gold Layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Create the final analytical layer with dimension and fact tables using DLT.
# MAGIC
# MAGIC In DLT, you reference tables from earlier in the pipeline using the `live.` prefix.

# COMMAND ----------

# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension Tables
# MAGIC
# MAGIC Dimension tables are denormalized, slowly changing reference tables.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="dim_customers",
# MAGIC     comment="Customer dimension table"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
# MAGIC def create_dim_customers():
# MAGIC     return (
# MAGIC         dlt.read("silver.customers")
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
# MAGIC     name="dim_stores",
# MAGIC     comment="Store dimension table"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
# MAGIC def create_dim_stores():
# MAGIC     return (
# MAGIC         dlt.read("silver.stores")
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
# MAGIC     name="dim_products",
# MAGIC     comment="Product dimension table"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
# MAGIC def create_dim_products():
# MAGIC     return (
# MAGIC         dlt.read("silver.products")
# MAGIC         .select(
# MAGIC             "id",
# MAGIC             "product_name",
# MAGIC             "product_category",
# MAGIC             "product_price",
# MAGIC             "etl_timestamp"
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact Table with Joins
# MAGIC
# MAGIC The fact table contains the core transaction data with foreign keys to dimensions.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="fact_iot_transactions",
# MAGIC     comment="IoT transaction fact table with dimension joins"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction", "transaction_amount IS NOT NULL AND transaction_date IS NOT NULL")
# MAGIC def create_fact_transactions():
# MAGIC     iot = dlt.read_stream("silver.iot_stream")
# MAGIC     customers = dlt.read("silver.customers")
# MAGIC     products = dlt.read("silver.products")
# MAGIC     stores = dlt.read("silver.stores")
# MAGIC
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

# MAGIC %md
# MAGIC ## SQL DLT Equivalent

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE dim_customers
# MAGIC AS SELECT * EXCEPT (date)
# MAGIC FROM live.silver_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE dim_stores
# MAGIC AS SELECT * EXCEPT (date)
# MAGIC FROM live.silver_stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE dim_products
# MAGIC AS SELECT * EXCEPT (date)
# MAGIC FROM live.silver_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE fact_iot_transactions
# MAGIC AS SELECT
# MAGIC     iot.id,
# MAGIC     iot.transaction_date,
# MAGIC     iot.transaction_amount,
# MAGIC     iot.customer_id,
# MAGIC     iot.product_id,
# MAGIC     iot.store_id,
# MAGIC     iot.etl_timestamp,
# MAGIC     c.customer_name,
# MAGIC     p.product_name,
# MAGIC     p.product_category,
# MAGIC     s.store_name,
# MAGIC     s.store_city
# MAGIC FROM live.silver_iot_stream iot
# MAGIC LEFT JOIN live.silver_customers c ON iot.customer_id = c.id
# MAGIC LEFT JOIN live.silver_products p ON iot.product_id = p.id
# MAGIC LEFT JOIN live.silver_stores s ON iot.store_id = s.id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized Views for Analytics
# MAGIC
# MAGIC Create aggregated views for specific use cases:

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="sales_daily_summary",
# MAGIC     comment="Daily sales summary for dashboards"
# MAGIC )
# MAGIC def create_daily_summary():
# MAGIC     return (
# MAGIC         dlt.read("fact_iot_transactions")
# MAGIC         .groupBy("transaction_date", "store_name", "product_category")
# MAGIC         .agg(
# MAGIC             F.count("*").alias("transaction_count"),
# MAGIC             F.sum("transaction_amount").alias("total_revenue"),
# MAGIC             F.avg("transaction_amount").alias("avg_transaction")
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE sales_daily_summary
# MAGIC AS SELECT
# MAGIC     transaction_date,
# MAGIC     store_name,
# MAGIC     product_category,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(transaction_amount) as total_revenue,
# MAGIC     AVG(transaction_amount) as avg_transaction
# MAGIC FROM live.fact_iot_transactions
# MAGIC GROUP BY transaction_date, store_name, product_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Graph
# MAGIC
# MAGIC After running your DLT pipeline, you can visualize the complete data flow:
# MAGIC
# MAGIC ```
# MAGIC Bronze (ingestion)
# MAGIC     |
# MAGIC     v
# MAGIC Silver (cleansing)
# MAGIC     |
# MAGIC     v
# MAGIC Gold (aggregation)
# MAGIC     |
# MAGIC     v
# MAGIC Fact & Dimensions
# MAGIC     |
# MAGIC     v
# MAGIC Analytics Views
# MAGIC ```
