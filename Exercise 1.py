# Databricks notebook source
# MAGIC %md
# MAGIC # Data ingstion

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data sources
# MAGIC - **Transaction data** - 
# MAGIC - **Customer data** - 
# MAGIC - **Product data** - 
# MAGIC - **Store data** - 

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

from databricks.sdk import WorkspaceClient

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

for i in w.external_locations.list():
    print(i)

# COMMAND ----------

w.external_locations.get("emhollanding").url

# COMMAND ----------



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


