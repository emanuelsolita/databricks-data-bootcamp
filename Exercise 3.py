# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3: Aggregation (Gold Layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC Create the Gold layer with dimension tables and a fact table, ready for analytics and reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this exercise, you'll create:
# MAGIC 1. **Dimension tables** (`dim_`) - Slowly changing reference data
# MAGIC 2. **Fact table** (`fact_`) - Transactional data with foreign keys
# MAGIC
# MAGIC All output should go to: `<catalog>.gold.<table_name>`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC The Gold layer joins data from all Silver tables:
# MAGIC
# MAGIC ```
# MAGIC Silver Tables          Gold Tables
# MAGIC ┌────────────────┐     ┌─────────────────┐
# MAGIC │ silver.customers │ ──▶ │ dim_customers    │
# MAGIC │ silver.products  │ ──▶ │ dim_products     │
# MAGIC │ silver.stores    │ ──▶ │ dim_stores       │
# MAGIC │ silver.iot_stream│ ──▶ │ fact_iot_data    │
# MAGIC └────────────────┘     └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ![medallion architecture](./docs/medallion_arch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create dim_customers
# MAGIC
# MAGIC Create a customer dimension table:
# MAGIC
# MAGIC Requirements:
# MAGIC - Select all columns from `silver.customers`
# MAGIC - Exclude the `date` column (not needed in Gold)
# MAGIC - Add a PRIMARY KEY constraint on `id`
# MAGIC
# MAGIC Use SQL:
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE <catalog>.<schema>.<table>
# MAGIC AS SELECT * EXCEPT(date) FROM <source>;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create dim_customers table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Add PRIMARY KEY constraint
# MAGIC ALTER TABLE emanuel_db.gold.dim_customers
# MAGIC ALTER COLUMN id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Add PRIMARY KEY
# MAGIC ALTER TABLE emanuel_db.gold.dim_customers
# MAGIC ADD CONSTRAINT dim_customers_pk PRIMARY KEY (id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Create dim_stores
# MAGIC
# MAGIC Create a store dimension table:
# MAGIC
# MAGIC Requirements:
# MAGIC - Select all columns from `silver.stores`
# MAGIC - Exclude the `date` column
# MAGIC - Add a PRIMARY KEY constraint on `id`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create dim_stores table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Add constraints
# MAGIC ALTER TABLE emanuel_db.gold.dim_stores
# MAGIC ALTER COLUMN id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE emanuel_db.gold.dim_stores
# MAGIC ADD CONSTRAINT dim_stores_pk PRIMARY KEY (id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Create dim_products
# MAGIC
# MAGIC Create a product dimension table:
# MAGIC
# MAGIC Requirements:
# MAGIC - Select all columns from `silver.products`
# MAGIC - Exclude the `date` column
# MAGIC - Add a PRIMARY KEY constraint on `id`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create dim_products table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Add constraints
# MAGIC ALTER TABLE .gold.dim_products
# MAGIC ALTER COLUMN id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE .gold.dim_products
# MAGIC ADD CONSTRAINT dim_products_pk PRIMARY KEY (id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Create fact_iot_data Table (DDL)
# MAGIC
# MAGIC Create the fact table with explicit schema and foreign key constraints:
# MAGIC
# MAGIC Requirements:
# MAGIC - Define columns explicitly with proper types
# MAGIC - Add PRIMARY KEY on `id`
# MAGIC - Add FOREIGN KEY constraints referencing the dimension tables:
# MAGIC   - `customer_id` → `dim_customers(id)`
# MAGIC   - `product_id` → `dim_products(id)`
# MAGIC   - `store_id` → `dim_stores(id)`
# MAGIC
# MAGIC Reference:
# MAGIC ```sql
# MAGIC CREATE TABLE t (
# MAGIC   id string PRIMARY KEY,
# MAGIC   customer_id bigint,
# MAGIC   CONSTRAINT fk_name FOREIGN KEY (customer_id) REFERENCES dim_customers(id)
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create fact table with constraints

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Populate fact_iot_data with Joins
# MAGIC
# MAGIC Insert data into the fact table by joining all Silver tables:
# MAGIC
# MAGIC Requirements:
# MAGIC - Select from `silver.iot_stream`
# MAGIC - Join with:
# MAGIC   - `silver.customers` → get `customer_name`
# MAGIC   - `silver.products` → get `product_name`
# MAGIC   - `silver.stores` → get `store_name`
# MAGIC - Use `INSERT OVERWRITE` to populate
# MAGIC
# MAGIC Reference:
# MAGIC ```sql
# MAGIC INSERT OVERWRITE TABLE target
# MAGIC SELECT ... FROM source1
# MAGIC JOIN source2 ON condition
# MAGIC ```

# COMMAND ----------

# TODO: Create fact table with joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Verify Gold Tables
# MAGIC
# MAGIC Check your Gold layer:
# MAGIC 1. List all Gold tables
# MAGIC 2. Check row counts
# MAGIC 3. Sample data from each table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: List gold tables
# MAGIC SHOW TABLES IN emanuel_db.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Count records in each table
# MAGIC SELECT 'dim_customers' as table_name, COUNT(*) as cnt FROM emanuel_db.gold.dim_customers
# MAGIC UNION ALL SELECT 'dim_products', COUNT(*) FROM emanuel_db.gold.dim_products
# MAGIC UNION ALL SELECT 'dim_stores', COUNT(*) FROM emanuel_db.gold.dim_stores
# MAGIC UNION ALL SELECT 'iot_data', COUNT(*) FROM emanuel_db.gold.iot_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Sample fact table
# MAGIC SELECT * FROM emanuel_db.gold.iot_data LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] `gold.dim_customers` created with PRIMARY KEY
# MAGIC - [ ] `gold.dim_products` created with PRIMARY KEY
# MAGIC - [ ] `gold.dim_stores` created with PRIMARY KEY
# MAGIC - [ ] `gold.iot_data` created with PRIMARY KEY and FOREIGN KEYS
# MAGIC - [ ] Fact table populated with joined data
# MAGIC - [ ] All Gold tables verified
