# Databricks notebook source
from pyspark import pipelines as dp

# Define the landing table as a streaming source (replace with your actual landing path)
@dp.view
def landing_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")  # Change format if needed
        .option("header", "true")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/")  # Update path as appropriate
    )

# Create the bronze table in emanuel_db schema
@dp.table(
    name="emanuel_db.bronze_dlt",
    comment="Bronze table ingested from landing zone"
)
def dlt_bronze():
    return spark.readStream.table("landing_data")

# COMMAND ----------

from pyspark.sql.functions import col

# Silver table with type casting and transformations
@dp.table(
    name="emanuel_db.silver_dlt",
    comment="Silver table with cleaned and casted columns"
)
def dlt_silver():
    bronze_df = spark.readStream.table("bronze_dlt")
    return (
        bronze_df
        .withColumn("unit_number", col("unit_number").cast("int"))
        .withColumn("time_cycles", col("time_cycles").cast("int"))
    )
