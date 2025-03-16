# Databricks notebook source
# MAGIC %md
# MAGIC # Data ingstion

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data sources
# MAGIC - **Transaction data** - abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/iot_stream/
# MAGIC - **Customer data** - abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/customers/
# MAGIC - **Product data** - abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/products/
# MAGIC - **Store data** - abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/stores/

# COMMAND ----------

# MAGIC %md
# MAGIC This exercies targets the first stage in the Medallion architecture, ingesting data from source to delta tables in `bronze` schema.
# MAGIC The data can be found here: [source data](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fae6cbacb-2eac-42cc-978e-516b8ef7628d%2FresourceGroups%2Femhol-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Flandingemhol/path/catalog/etag/%220x8DC5D4A6D306AE3%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None).
# MAGIC
# MAGIC ![medallion architecture](./docs/medallion_arch.png)
# MAGIC
# MAGIC To be able to access this storage location, an external location is needed. This has been created for you and can be viewed here [External location to external storage](https://adb-8983212560648347.7.azuredatabricks.net/explore/locations/emhollanding?o=8983212560648347).
# MAGIC
# MAGIC To programatically get the external location url you can use the package ```databricks.sdk``` and the class ```w = WorkspaceClient()```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task
# MAGIC
# MAGIC Use PySpark or Spark SQL to ingest data from source to bronze.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

for i in w.external_locations.list():
    print(i)

# COMMAND ----------

w.external_locations.get("emhollanding").url

# COMMAND ----------

# MAGIC %md
# MAGIC ### Structured Streaming and Batch loads.
# MAGIC
# MAGIC Set up the ingestion step for the different data sources.
# MAGIC Use Autoloader (```spark.readStream...```) for the transaction source and "batch mode" (```spark.read...```) for the dimensional data sources.
# MAGIC
# MAGIC
# MAGIC To complete the ingestion you also need to write the stream/batch to a target table (```spark_df.writeStream...```/```spark_df.write...```). We choose to write the source data to a delta table in our catalog and ```bronze``` schema. 
# MAGIC
# MAGIC We refer to tables with its three-level-space name e.g. ```emanuel_db.bronze.test_table```
# MAGIC ![](./docs/catalog_schema.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction Data (IoT stream)

# COMMAND ----------

df_trans = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            #.option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/tmp/schema")
            .load(f"{w.external_locations.get('emhollanding').url}bootcamp/iot_stream/")
            )

(df_trans.writeStream
 .option("checkpointLocation", "/tmp/iot_stream_checkpoint")
 .option("partitionBy", "transaction_date")
 .trigger(once=True)
 .table("emanuel_db.bronze.iot_stream")
)
            

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up schema and checkpoint location

# COMMAND ----------

dbutils.fs.rm("/tmp/iot_stream_checkpoint", True)
dbutils.fs.rm("/tmp/schema", True)
spark.sql("DROP TABLE IF EXISTS emanuel_db.bronze.iot_stream")

# COMMAND ----------

df_trans.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Write Customers - Batch
# MAGIC
# MAGIC **Read**
# MAGIC
# MAGIC PySpark
# MAGIC `spark.read.json("path")`
# MAGIC
# MAGIC Spark SQL
# MAGIC ```
# MAGIC SELECT * FROM json.`abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/customers/`
# MAGIC ```
# MAGIC
# MAGIC **Write**
# MAGIC
# MAGIC *PySpark*
# MAGIC `spark_df.write.saveAsTable("catalog.schema.table")`
# MAGIC
# MAGIC *Spark SQL*
# MAGIC
# MAGIC `CREATE TABLE emanuel_db.bronze.customers;`
# MAGIC
# MAGIC ```
# MAGIC COPY INTO emanuel_db.bronze.customers
# MAGIC FROM 'abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/customers/'
# MAGIC FILEFORMAT = JSON;
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/customers/`

# COMMAND ----------

# Reading Batch Data
df_cus = spark.read.json(f"{w.external_locations.get('emhollanding').url}bootcamp/customers/")
df_cus.display()

# COMMAND ----------

# Writing to a Table
df_cus.write.mode("overwrite").saveAsTable("emanuel_db.bronze.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO emanuel_db.bronze.customers
# MAGIC FROM 'abfss://catalog@landingemhol.dfs.core.windows.net/bootcamp/customers/'
# MAGIC FILEFORMAT = JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Write Products

# COMMAND ----------

df_prod = spark.read.json(f"{w.external_locations.get('emhollanding').url}bootcamp/products/")
df_prod.display()

# COMMAND ----------

df_prod.write.mode("overwrite").saveAsTable("emanuel_db.bronze.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Write Stores

# COMMAND ----------

df_stores = spark.read.json(f"{w.external_locations.get('emhollanding').url}bootcamp/stores/")
df_stores.display()

# COMMAND ----------

df_stores.write.mode("overwrite").saveAsTable("emanuel_db.bronze.stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect your new tables in under catalogs

# COMMAND ----------


