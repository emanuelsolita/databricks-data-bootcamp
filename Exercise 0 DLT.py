# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to Spark Declarative Pipelines (Delta Live Tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Spark Declarative Pipelines?
# MAGIC
# MAGIC Spark Declarative Pipelines, also known as **Delta Live Tables (DLT)**, allow you to define data pipelines using a declarative syntax rather than imperative PySpark code.
# MAGIC
# MAGIC Instead of:
# MAGIC - Reading data manually
# MAGIC - Writing transformation logic step-by-step
# MAGIC - Managing checkpoints and streaming queries separately
# MAGIC
# MAGIC You simply:
# MAGIC - Define **tables** and their **transformations**
# MAGIC - DLT handles the execution, optimization, and data quality
# MAGIC
# MAGIC Benefits:
# MAGIC - **Simpler syntax** - focus on "what" not "how"
# MAGIC - **Built-in data quality** - expect clauses catch bad data
# MAGIC - **Automatic optimization** - DLT optimizes the pipeline automatically
# MAGIC - **Automatic schema evolution** - handles changing data sources

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT vs PySpark
# MAGIC
# MAGIC | Aspect | PySpark | DLT |
# MAGIC |--------|---------|-----|
# MAGIC | Syntax | Imperative | Declarative |
# MAGIC | Read/Write | Manual | Automatic |
# MAGIC | Checkpoints | Manual | Automatic |
# MAGIC | Data Quality | Manual | Built-in |
# MAGIC | Schema Evolution | Manual | Automatic |

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Syntax Overview

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python DLT Syntax
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table
# MAGIC def table_name():
# MAGIC     return spark.read.source("source_type").load("path")
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
# MAGIC def cleaned_table():
# MAGIC     return dlt.read("source_table").filter("id IS NOT NULL")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL DLT Syntax
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE table_name
# MAGIC AS SELECT * FROM source;
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE cleaned_table
# MAGIC AS SELECT * FROM live.source_table
# MAGIC WHERE id IS NOT NULL;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key DLT Concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. LIVE Keyword
# MAGIC Use `live.` prefix to reference other tables in your pipeline:
# MAGIC ```sql
# MAGIC SELECT * FROM live.bronze_table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Data Expectations
# MAGIC Define data quality rules:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE cleaned_table
# MAGIC EXPECTING (id IS NOT NULL)  -- Drops invalid rows
# MAGIC AS SELECT * FROM live.source;
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE report
# MAGIC EXPECTING (revenue > 0) ON VIOLATION UPDATE ('ignore')  -- Keeps invalid but flags them
# MAGIC AS SELECT * FROM live.cleaned;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Autoloader in DLT
# MAGIC Cloud Files syntax made simple:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def bronze_table():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .load("path/to/data")
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Use DLT vs PySpark?
# MAGIC
# MAGIC **Use DLT when:**
# MAGIC - Building production data pipelines
# MAGIC - Data quality is important
# MAGIC - You want automatic schema evolution
# MAGIC - You're working with streaming data
# MAGIC
# MAGIC **Use PySpark when:**
# MAGIC - Learning Spark fundamentals
# MAGIC - Ad-hoc data exploration
# MAGIC - Complex custom transformations
# MAGIC - One-off batch processing

# COMMAND ----------
