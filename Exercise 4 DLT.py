# Databricks notebook source
# MAGIC %md
# MAGIC # Create a DLT Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC This exercise creates a complete Delta Live Tables pipeline that combines the bronze, silver, and gold layers from the previous exercises.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Pipeline
# MAGIC
# MAGIC 1. Go to **Delta Live Tables** in the sidebar
# MAGIC 2. Click **Create Pipeline**
# MAGIC
# MAGIC ![](./docs/dlt_create_pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Settings

# COMMAND ----------

# MAGIC %md
# MAGIC Configure your pipeline:
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Pipeline Name** | `retail_analytics_dlt` |
# MAGIC | **Pipeline Mode** | `Continuous` (for streaming) or `Triggered` (for batch) |
# MAGIC | **Target** | `emanuel_db` (your catalog) |
# MAGIC | **Storage** | Leave empty to use auto-generated |
# MAGIC | **Notebooks** | Add this notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Modes
# MAGIC
# MAGIC **Continuous Mode:**
# MAGIC - Runs continuously, processing new data as it arrives
# MAGIC - Best for real-time streaming data
# MAGIC - Higher cost (cluster stays running)
# MAGIC
# MAGIC **Triggered Mode:**
# MAGIC - Runs once, then stops
# MAGIC - Best for batch processing
# MAGIC - Lower cost (cluster terminates after run)
# MAGIC - Can be scheduled for regular intervals

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add All Notebooks
# MAGIC
# MAGIC You can split your pipeline across multiple notebooks:
# MAGIC
# MAGIC | Notebook | Tables Created |
# MAGIC |----------|---------------|
# MAGIC | `Exercise 1 DLT` | Bronze layer tables |
# MAGIC | `Exercise 2 DLT` | Silver layer tables |
# MAGIC | `Exercise 3 DLT` | Gold layer tables |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Pipeline Example
# MAGIC
# MAGIC Here's a complete, self-contained DLT pipeline:

# COMMAND ----------

# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %python
# MAGIC # ============================================
# MAGIC # BRONZE LAYER - Data Ingestion
# MAGIC # ============================================

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="bronze_iot_stream", comment="Raw IoT transaction stream")
# MAGIC def bronze_ingest_iot():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="bronze_customers", comment="Raw customer data")
# MAGIC def bronze_ingest_customers():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="bronze_products", comment="Raw product data")
# MAGIC def bronze_ingest_products():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="bronze_stores", comment="Raw store data")
# MAGIC def bronze_ingest_stores():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC # ============================================
# MAGIC # SILVER LAYER - Data Cleansing
# MAGIC # ============================================

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="silver_iot_stream", comment="Cleaned transaction stream")
# MAGIC @dlt.expect_or_drop("valid_amount", "transaction_amount IS NOT NULL")
# MAGIC def silver_clean_iot():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze_iot_stream")
# MAGIC         .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
# MAGIC         .withColumn("transaction_date", F.to_date("transaction_date"))
# MAGIC         .withColumn("customer_id", F.col("customer_id").cast("long"))
# MAGIC         .withColumn("product_id", F.col("product_id").cast("long"))
# MAGIC         .withColumn("store_id", F.col("store_id").cast("long"))
# MAGIC     )

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="silver_customers", comment="Deduplicated customers")
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def silver_clean_customers():
# MAGIC     return dlt.read("bronze_customers").dropDuplicates(["id"])

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="silver_products", comment="Deduplicated products")
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def silver_clean_products():
# MAGIC     return dlt.read("bronze_products").dropDuplicates(["id"])

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="silver_stores", comment="Deduplicated stores")
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def silver_clean_stores():
# MAGIC     return dlt.read("bronze_stores").dropDuplicates(["id"])

# COMMAND ----------

# MAGIC %python
# MAGIC # ============================================
# MAGIC # GOLD LAYER - Analytics
# MAGIC # ============================================

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="dim_customers", comment="Customer dimension")
# MAGIC def gold_dim_customers():
# MAGIC     return dlt.read("silver_customers")

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="dim_products", comment="Product dimension")
# MAGIC def gold_dim_products():
# MAGIC     return dlt.read("silver_products")

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="dim_stores", comment="Store dimension")
# MAGIC def gold_dim_stores():
# MAGIC     return dlt.read("silver_stores")

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(name="fact_transactions", comment="Transaction fact table")
# MAGIC def gold_fact_transactions():
# MAGIC     iot = dlt.read_stream("silver_iot_stream")
# MAGIC     cust = dlt.read("silver_customers").select("id", "customer_name")
# MAGIC     prod = dlt.read("silver_products").select("id", "product_name")
# MAGIC     stor = dlt.read("silver_stores").select("id", "store_name")
# MAGIC
# MAGIC     return (
# MAGIC         iot
# MAGIC         .join(cust, iot.customer_id == cust.id, "left")
# MAGIC         .join(prod, iot.product_id == prod.id, "left")
# MAGIC         .join(stor, iot.store_id == stor.id, "left")
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Your Pipeline
# MAGIC
# MAGIC After creating and running the pipeline:
# MAGIC
# MAGIC 1. **Pipeline Graph** - Visualize data flow
# MAGIC 2. **Data Quality** - See expectation results
# MAGIC 3. **Metrics** - Track records processed
# MAGIC 4. **Logs** - Debug any issues

# COMMAND ----------
