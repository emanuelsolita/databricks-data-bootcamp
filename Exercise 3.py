# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC Create a consumption ready data views.
# MAGIC
# MAGIC Here we want to create a consolidated view containing transaction data, product data, customer data, and store data.
# MAGIC
# MAGIC Join these table together and create a view or a table.
# MAGIC
# MAGIC Make sure the view does not contain any duplicates based on `transaction_id`. 

# COMMAND ----------

# MAGIC %md
# MAGIC If we choose to create a view instead of a table we can switch the default language to `SQL` in the top left corner.
# MAGIC
# MAGIC ![](./docs/change_language.png)
# MAGIC
# MAGIC
# MAGIC Or directly run SQL in a python notebooks by decorating the top of the cell with:
# MAGIC > `%sql`
# MAGIC
# MAGIC followed by the SQL-query e.g.
# MAGIC ```
# MAGIC %sql
# MAGIC create or replace view <catalog>.<schema>.<table>
# MAGIC as
# MAGIC select 
# MAGIC   *
# MAGIC from ...
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table emanuel_db.gold.dim_customers as
# MAGIC select * except(date) from emanuel_db.silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE emanuel_db.gold.dim_customers
# MAGIC ALTER COLUMN id SET NOT NULL;
# MAGIC ALTER TABLE emanuel_db.gold.dim_customers
# MAGIC ADD CONSTRAINT dim_customers_pk PRIMARY KEY (id);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table emanuel_db.gold.dim_stores as
# MAGIC select * except(date) from emanuel_db.silver.stores;
# MAGIC ALTER TABLE emanuel_db.gold.dim_stores
# MAGIC ALTER COLUMN id SET NOT NULL;
# MAGIC ALTER TABLE emanuel_db.gold.dim_stores
# MAGIC ADD CONSTRAINT dim_stores_pk PRIMARY KEY (id);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table emanuel_db.gold.dim_products as
# MAGIC select * except(date) from emanuel_db.silver.products;
# MAGIC ALTER TABLE emanuel_db.gold.dim_products
# MAGIC ALTER COLUMN id SET NOT NULL;
# MAGIC ALTER TABLE emanuel_db.gold.dim_products
# MAGIC ADD CONSTRAINT dim_products_pk PRIMARY KEY (id);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # DDL for IoT stream

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table emanuel_db.gold.iot_data;
# MAGIC CREATE OR REPLACE TABLE emanuel_db.gold.iot_data (
# MAGIC   id string primary key,
# MAGIC   customer_id bigint,
# MAGIC   product_id bigint,
# MAGIC   receipt_id bigint,
# MAGIC   store_id bigint,
# MAGIC   transaction_amount double,
# MAGIC   transaction_date date,
# MAGIC   etl_timestamp timestamp,
# MAGIC   customer_name string,
# MAGIC   product_name string,
# MAGIC   store_name string,
# MAGIC   CONSTRAINT emanuel_db_gold_stores_store_id_fk FOREIGN KEY (store_id) REFERENCES emanuel_db.gold.dim_stores(id),
# MAGIC   CONSTRAINT emanuel_db_gold_products_product_id_fk FOREIGN KEY (product_id) REFERENCES emanuel_db.gold.dim_products(id),
# MAGIC   CONSTRAINT emanuel_db_gold_customers_customer_id_fk FOREIGN KEY (customer_id) REFERENCES emanuel_db.gold.dim_customers(id)
# MAGIC
# MAGIC )

# COMMAND ----------

query = """
    INSERT OVERWRITE TABLE emanuel_db.gold.iot_data
    SELECT 
        iot.id,
        customer_id,
        product_id,
        receipt_id,
        store_id,
        transaction_amount,
        transaction_date,
        iot.etl_timestamp,
        c.customer_name,
        p.product_name,
        s.store_name
    FROM 
        emanuel_db.silver.iot_stream AS iot
    JOIN 
        emanuel_db.silver.customers AS c
    ON 
        iot.customer_id = c.id
    JOIN 
        emanuel_db.silver.products AS p
    ON 
        iot.product_id = p.id
    JOIN 
        emanuel_db.silver.stores AS s
    ON 
        iot.store_id = s.id
"""

spark.sql(query)

# COMMAND ----------


