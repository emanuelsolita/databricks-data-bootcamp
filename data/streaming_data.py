# Databricks notebook source
import numpy as np
import pandas as pd
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------

ext_loc_url = w.external_locations.get("emhollanding").url

# COMMAND ----------

# MAGIC %md
# MAGIC Trasnactional data for products sold in stores, simulated IoT stream.
# MAGIC
# MAGIC Attributes: 
# MAGIC ```
# MAGIC id: unique transaction id
# MAGIC product_id: unique product id
# MAGIC customer_id: unique customer id
# MAGIC store_id: unique store id
# MAGIC receipt_id: unique receipt id, collection of products in a store done by a customer at one time
# MAGIC transaction_date: date of transaction
# MAGIC transaction_amount: price per product
# MAGIC ```

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, expr
import uuid
from datetime import datetime, timedelta
import random

# Function to generate random date
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 3, 10)
def random_date():
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

# Generate 1,000 rows
data = [
    (
        str(uuid.uuid4()),  # transaction_id
        random.randint(1, 150),  # product_id
        random.randint(1, 100),  # customer_id
        random.randint(1, 10),  # store_id
        random.randint(1000, 9999),  # receipt_id
        random_date().strftime("%Y-%m-%d"),  # transaction_date
        round(random.uniform(5, 500), 2)  # transaction_amount
    )
    for i in range(50000, 51000, 1)
]

# Define schema and create DataFrame
columns = ["id", "product_id", "customer_id", "store_id", "receipt_id", "transaction_date", "transaction_amount"]
df = spark.createDataFrame(data, columns)


# COMMAND ----------

df.write.mode("overwrite").format("json").option("partitionBy", "transaction_date").save(f"{ext_loc_url}/bootcamp/iot_stream_2/")

# COMMAND ----------


