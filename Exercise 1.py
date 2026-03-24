# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: Data Ingestion (Bronze Layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC Ingest raw data from external sources into Delta tables in the Bronze layer using PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Sources
# MAGIC | Source | Path |
# MAGIC |--------|------|
# MAGIC | Transaction data | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/` |
# MAGIC | Customer data | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/` |
# MAGIC | Product data | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/` |
# MAGIC | Store data | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/` |
# MAGIC
# MAGIC The data is stored in Azure Data Lake Storage. To access it programmatically, use the Databricks SDK.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC This exercise implements the **Bronze layer** of the Medallion architecture:
# MAGIC
# MAGIC ![medallion architecture](./docs/medallion_arch.png)
# MAGIC
# MAGIC All ingested data should go to: `<catalog>.bronze.<table_name>`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Get External Location URL
# MAGIC
# MAGIC Use the Databricks SDK to programmatically access the external location:
# MAGIC
# MAGIC 1. Import `WorkspaceClient` from `databricks.sdk`
# MAGIC 2. Create a `WorkspaceClient` instance
# MAGIC 3. Get the external location URL by name
# MAGIC
# MAGIC Reference: `WorkspaceClient()`, `external_locations.get()`

# COMMAND ----------


# TODO: Get external location URL
from databricks.sdk import WorkspaceClient

# COMMAND ----------


# TODO: Get the landing URL
w = WorkspaceClient()
landing_url = w.external_locations.get("landingneusa").url
print(landing_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Ingest Transaction Data (Streaming)
# MAGIC
# MAGIC Use **Autoloader** (`cloudFiles`) to ingest the IoT transaction stream:
# MAGIC
# MAGIC Requirements:
# MAGIC - Read as a **stream** using `spark.readStream`
# MAGIC - Use `cloudFiles` format with JSON
# MAGIC - Add a schema location for inference
# MAGIC - Add an `etl_timestamp` column with current timestamp
# MAGIC - Write to `emanuel_db.bronze.iot_stream`
# MAGIC - Set a checkpoint location
# MAGIC - Use `.trigger(once=True)` for batch-like behavior
# MAGIC
# MAGIC Reference: `spark.readStream`, `format("cloudFiles")`, `writeStream`, `checkpointLocation`

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Ingest Customer Data (Batch)
# MAGIC
# MAGIC Use **batch mode** to read the customer data:
# MAGIC
# MAGIC Requirements:
# MAGIC - Read as **batch** using `spark.read`
# MAGIC - Read JSON files from the customers path
# MAGIC - Add an `etl_timestamp` column
# MAGIC - Write to `emanuel_db.bronze.customers`
# MAGIC
# MAGIC Reference: `spark.read.json()`, `write.mode("overwrite")`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Ingest Product Data (Batch)
# MAGIC
# MAGIC Repeat the same pattern for products:
# MAGIC
# MAGIC Requirements:
# MAGIC - Read JSON from the products path
# MAGIC - Add `etl_timestamp` column
# MAGIC - Write to `emanuel_db.bronze.products`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Ingest Store Data (Batch)
# MAGIC
# MAGIC Complete the Bronze layer by ingesting store data:
# MAGIC
# MAGIC Requirements:
# MAGIC - Read JSON from the stores path
# MAGIC - Add `etl_timestamp` column
# MAGIC - Write to `emanuel_db.bronze.stores`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Inspect Bronze Tables
# MAGIC
# MAGIC Verify your tables were created correctly:
# MAGIC 1. Check the schema of each table
# MAGIC 2. Preview the data
# MAGIC 3. Count the records
# MAGIC
# MAGIC Tables to verify:
# MAGIC - `emanuel_db.bronze.iot_stream`
# MAGIC - `emanuel_db.bronze.customers`
# MAGIC - `emanuel_db.bronze.products`
# MAGIC - `emanuel_db.bronze.stores`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Check bronze tables exist
# MAGIC SHOW TABLES IN emanuel_db.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Count records in each table
# MAGIC SELECT 'iot_stream' as table_name, COUNT(*) as record_count FROM emanuel_db.bronze.iot_stream
# MAGIC UNION ALL SELECT 'customers', COUNT(*) FROM emanuel_db.bronze.customers
# MAGIC UNION ALL SELECT 'products', COUNT(*) FROM emanuel_db.bronze.products
# MAGIC UNION ALL SELECT 'stores', COUNT(*) FROM emanuel_db.bronze.stores;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC If you need to rerun the exercise, you can clean up with:
# MAGIC
# MAGIC ```python
# MAGIC dbutils.fs.rm("/tmp/iot_stream_checkpoint", True)
# MAGIC dbutils.fs.rm("/tmp/schema", True)
# MAGIC spark.sql("DROP TABLE IF EXISTS emanuel_db.bronze.iot_stream")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] External location URL retrieved
# MAGIC - [ ] `bronze.iot_stream` created (streaming)
# MAGIC - [ ] `bronze.customers` created (batch)
# MAGIC - [ ] `bronze.products` created (batch)
# MAGIC - [ ] `bronze.stores` created (batch)
# MAGIC - [ ] All tables verified
