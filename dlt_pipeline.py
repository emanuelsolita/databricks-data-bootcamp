# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline Template - Retail Analytics
# MAGIC
# MAGIC This is a template for the Delta Live Tables pipeline. Follow the instructions in DLT_SETUP_GUIDE.md to complete the implementation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tips for Completing This Exercise
# MAGIC
# MAGIC 1. **Import the pipelines module**: Use `from pyspark import pipelines as dp`
# MAGIC 2. **Define tables using decorators**: `@dp.table(name="...")`
# MAGIC 3. **Add data quality expectations**: `@dp.expect_or_drop("name", "condition")`
# MAGIC 4. **Read from other DLT tables**: `dp.read("table_name")` or `dp.read_stream("table_name")`

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE SCHEMA - Data Ingestion
# MAGIC
# MAGIC TODO: Define tables for:
# MAGIC - iot_stream_dlt (streaming using spark.readStream and cloudFiles)
# MAGIC - customers_dlt (batch)
# MAGIC - products_dlt (batch)
# MAGIC - stores_dlt (batch)
# MAGIC
# MAGIC Data sources:
# MAGIC - IoT Stream: `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/`
# MAGIC - Customers: `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/`
# MAGIC - Products: `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/`
# MAGIC - Stores: `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/`

# COMMAND ----------


# TODO: Implement bronze_iot_stream_dlt() using @dp.table decorator
# Hint: Use spark.readStream with cloudFiles format for JSON
# Expected: Table at emanuel_db.bronze.iot_stream_dlt
def bronze_iot_stream_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement bronze_customers_dlt()
# Hint: Use spark.read for batch ingestion
# Expected: Table at emanuel_db.bronze.customers_dlt
def bronze_customers_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement bronze_products_dlt()
# Hint: Use spark.read for batch ingestion
# Expected: Table at emanuel_db.bronze.products_dlt
def bronze_products_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement bronze_stores_dlt()
# Hint: Use spark.read for batch ingestion
# Expected: Table at emanuel_db.bronze.stores_dlt
def bronze_stores_dlt():
    pass  # TODO: Implement


# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER SCHEMA - Data Transformation
# MAGIC
# MAGIC TODO: Define tables for:
# MAGIC - silver_iot_stream_dlt (clean data types, handle streaming)
# MAGIC - silver_customers_dlt (deduplicate, validate)
# MAGIC - silver_products_dlt (deduplicate, validate)
# MAGIC - silver_stores_dlt (deduplicate, validate)
# MAGIC
# MAGIC Tips:
# MAGIC - Use dp.read() for static tables
# MAGIC - Use dp.read_stream() for tables from streaming sources
# MAGIC - Apply type casting with .cast()
# MAGIC - Use .dropDuplicates() for deduplication

# COMMAND ----------


# TODO: Implement silver_iot_stream_dlt()
# Hint: Read from bronze_iot_stream_dlt, apply type casting
# Expected: Table at emanuel_db.silver.iot_stream_dlt
def silver_iot_stream_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement silver_customers_dlt()
# Hint: Read from bronze_customers_dlt, deduplicate, add expectations
# Expected: Table at emanuel_db.silver.customers_dlt
def silver_customers_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement silver_products_dlt()
# Hint: Read from bronze_products_dlt, deduplicate
# Expected: Table at emanuel_db.silver.products_dlt
def silver_products_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement silver_stores_dlt()
# Hint: Read from bronze_stores_dlt, deduplicate
# Expected: Table at emanuel_db.silver.stores_dlt
def silver_stores_dlt():
    pass  # TODO: Implement


# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD SCHEMA - Dimension and Fact Tables
# MAGIC
# MAGIC TODO: Define tables for:
# MAGIC - dim_customers_dlt (dimension table)
# MAGIC - dim_stores_dlt (dimension table)
# MAGIC - dim_products_dlt (dimension table)
# MAGIC - fact_iot_transactions_dlt (fact table with joins)
# MAGIC - sales_daily_summary_dlt (aggregation)
# MAGIC
# MAGIC Tips:
# MAGIC - Dimension tables: select relevant columns
# MAGIC - Fact tables: join dimensions and select facts
# MAGIC - Aggregations: use groupBy with agg()

# COMMAND ----------


# TODO: Implement gold_dim_customers_dlt()
# Hint: Read from silver_customers_dlt, select id and relevant columns
# Expected: Table at emanuel_db.gold.dim_customers_dlt
def gold_dim_customers_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement gold_dim_stores_dlt()
# Hint: Read from silver_stores_dlt, select id and relevant columns
# Expected: Table at emanuel_db.gold.dim_stores_dlt
def gold_dim_stores_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement gold_dim_products_dlt()
# Hint: Read from silver_products_dlt, select id and relevant columns
# Expected: Table at emanuel_db.gold.dim_products_dlt
def gold_dim_products_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement gold_fact_iot_transactions_dlt()
# Hint: Join silver_iot_stream_dlt with dimension tables, select facts and keys
# Expected: Table at emanuel_db.gold.fact_iot_transactions_dlt
def gold_fact_iot_transactions_dlt():
    pass  # TODO: Implement


# COMMAND ----------


# TODO: Implement gold_sales_daily_summary_dlt()
# Hint: Aggregate fact table by date, store, and category
# Expected: Table at emanuel_db.gold.sales_daily_summary_dlt
def gold_sales_daily_summary_dlt():
    pass  # TODO: Implement


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference: DLT Decorators and Functions
# MAGIC
# MAGIC | Decorator/Function | Usage |
# MAGIC |---------------------|-------|
# MAGIC | `@dp.table(name="...")` | Define a DLT table |
# MAGIC | `@dp.expect(name, condition)` | Add expectation (records violations) |
# MAGIC | `@dp.expect_or_drop(name, condition)` | Drop rows that violate |
# MAGIC | `@dp.expect_or_fail(name, condition)` | Fail pipeline on violation |
# MAGIC | `dp.read(table_name)` | Read a DLT table |
# MAGIC | `dp.read_stream(table_name)` | Read a streaming DLT table |
# MAGIC
# MAGIC ## Reference: Data Sources
# MAGIC
# MAGIC | Source | Path |
# MAGIC |--------|------|
# MAGIC | IoT Stream | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/` |
# MAGIC | Customers | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/` |
# MAGIC | Products | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/` |
# MAGIC | Stores | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/` |
