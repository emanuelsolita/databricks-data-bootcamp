# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion with DLT (Bronze Layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data sources
# MAGIC - **Transaction data** - abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/
# MAGIC - **Customer data** - abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/
# MAGIC - **Product data** - abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/
# MAGIC - **Store data** - abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/

# COMMAND ----------

# MAGIC %md
# MAGIC This exercise targets the first stage in the Medallion architecture using **Delta Live Tables**.
# MAGIC
# MAGIC Instead of writing imperative PySpark code, we define tables and DLT handles:
# MAGIC - Automatic streaming/batch detection
# MAGIC - Checkpoint management
# MAGIC - Schema evolution
# MAGIC - Data quality enforcement

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline Setup
# MAGIC
# MAGIC To create a DLT pipeline:
# MAGIC 1. Go to **Delta Live Tables** in the sidebar
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Add this notebook as the pipeline
# MAGIC 4. Set the target catalog/schema: `emanuel_db.bronze`
# MAGIC 5. Add the external location path in pipeline settings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python DLT Implementation

# COMMAND ----------

# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Transaction Data (Streaming)
# MAGIC Using Autoloader with cloudFiles - DLT handles checkpointing automatically!

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="iot_stream",
# MAGIC     comment="Raw IoT transaction stream from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_transaction_id", "id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
# MAGIC def ingest_iot_stream():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Customer Data (Batch)
# MAGIC DLT automatically detects if this is streaming or batch based on the source!

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="customers",
# MAGIC     comment="Raw customer data from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_customer_id", "id IS NOT NULL")
# MAGIC def ingest_customers():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Product Data

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="products",
# MAGIC     comment="Raw product data from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_product_id", "id IS NOT NULL")
# MAGIC def ingest_products():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Store Data

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.table(
# MAGIC     name="stores",
# MAGIC     comment="Raw store data from landing zone"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_store_id", "id IS NOT NULL")
# MAGIC def ingest_stores():
# MAGIC     return (
# MAGIC         spark.read
# MAGIC         .format("json")
# MAGIC         .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/")
# MAGIC         .withColumn("etl_timestamp", F.current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL DLT Implementation
# MAGIC
# MAGIC The same pipeline can be written entirely in SQL!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: SQL DLT doesn't support Autoloader directly
# MAGIC -- For streaming sources in SQL, use COPY INTO instead
# MAGIC
# MAGIC -- For batch sources, you can use:
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC AS SELECT * FROM json.`abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Features
# MAGIC
# MAGIC DLT provides powerful data quality features:
# MAGIC
# MAGIC | Method | Behavior |
# MAGIC |--------|---------|
# MAGIC | `@dlt.expect()` | Records violations but keeps rows |
# MAGIC | `@dlt.expect_or_drop()` | Removes invalid rows |
# MAGIC | `@dlt.expect_or_fail()` | Stops pipeline on violations |
# MAGIC
# MAGIC Example with custom expectations:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect("positive_amount", "amount > 0")
# MAGIC @dlt.expect("valid_email", "email LIKE '%@%'")
# MAGIC def validated_data():
# MAGIC     return dlt.read("source_table")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Your Pipeline
# MAGIC
# MAGIC After running the pipeline, you can:
# MAGIC - View pipeline graphs and data lineage
# MAGIC - Monitor data quality metrics
# MAGIC - See schema evolution history
