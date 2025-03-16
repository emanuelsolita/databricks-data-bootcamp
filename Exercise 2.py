# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation 

# COMMAND ----------

# MAGIC %md
# MAGIC In this Exercise we explore the data we wrote to our raw layer (bronze) a little further. We look out for duplicates, data types and other formatting issues that the raw data might experience.
# MAGIC
# MAGIC The final ouput of this exercise will be the final datasets that does not contain any duplicate values and has proper data types. These should then be written to the ```silver``` schema in your Catalog. 
# MAGIC
# MAGIC Please use **Exercise 0** as reference for different transformations

# COMMAND ----------

# Read the tables to inspect the data (e.g. summarize to view the data composition) and its metadata (e.g. data types)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read transaction stream and write it to silver
# MAGIC For the transaction table no transformations are needed

# COMMAND ----------

from pyspark.sql import functions as F

df_trans = (spark.readStream
            .table("emanuel_db.bronze.iot_stream"))

df_trans = df_trans.withColumn("transaction_amount", F.col("transaction_amount").cast("double"))

(df_trans.writeStream 
    .format("delta")
    .option("checkpointLocation", f"/usr/local/checkpoint/iot_stream")
    .option("overwriteSchema", "true") #change column name or type explicitly and allow for overwriting the schema in the output table
    .option("skipChangeCommits", 'true')
    .outputMode("append")
    #.option("mergeSchema", "true") #whether to infer the schema across multiple files and to merge the schema of each file in the output table
    .trigger(once=True) 
    .table(f"emanuel_db.silver.iot_stream"))

# COMMAND ----------

dbutils.fs.rm("/usr/local/checkpoint/iot_stream", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.silver.iot_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and explore Customers, Products and Stores
# MAGIC Some things to look for
# MAGIC - Appropriate data types
# MAGIC - Duplicates
# MAGIC
# MAGIC
# MAGIC Make appropriate changes and write to silver schema. 
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

df_customers = spark.read.table("emanuel_db.bronze.customers")

df_customers.display()

# COMMAND ----------

df_customers = df_customers.dropDuplicates(["id"])
df_customers.display()

# COMMAND ----------

(df_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable("emanuel_db.silver.customers"))

# COMMAND ----------

df_products = spark.read.table("emanuel_db.bronze.products")
df_products = df_products.dropDuplicates(["id"])
df_products.display()

(df_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable("emanuel_db.silver.products"))

# COMMAND ----------

df_stores = spark.read.table("emanuel_db.bronze.stores")
df_stores = df_stores.dropDuplicates(["id"])
display(df_stores)

(df_stores.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable("emanuel_db.silver.stores"))
