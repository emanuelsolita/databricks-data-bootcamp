# Delta Live Tables Exercise Guide

This guide provides setup instructions and tips for completing the DLT pipeline exercise.

## Exercise Overview

In this exercise, you will build a complete Delta Live Tables pipeline implementing the Medallion architecture:
- **Bronze**: Raw data ingestion
- **Silver**: Data cleansing and transformation
- **Gold**: Business-ready dimension and fact tables

## Prerequisites

Before starting:
- [ ] Access to Azure Databricks workspace
- [ ] Unity Catalog configured with your catalog
- [ ] Storage account access for the landing zone
- [ ] Cluster created with appropriate runtime

## Setup Steps

### Step 1: Create Schemas

Create the necessary schemas in Unity Catalog:

```sql
CREATE SCHEMA IF NOT EXISTS emanuel_db.bronze;
CREATE SCHEMA IF NOT EXISTS emanuel_db.silver;
CREATE SCHEMA IF NOT EXISTS emanuel_db.gold;
```

### Step 2: Create DLT Pipeline

1. Navigate to **Delta Live Tables** in the Databricks sidebar
2. Click **Create Pipeline**
3. Configure:
   - **Pipeline Name**: `retail_analytics_dlt`
   - **Product**: `DLT`
   - **Pipeline Mode**: `Triggered` (recommended) or `Continuous`
   - **Target Catalog**: `emanuel_db`
   - **Target Schema**: Leave empty (tables use 3-level namespace)
   - **Storage**: Add your storage path for pipeline artifacts

### Step 3: Add Notebook

In pipeline settings, add `dlt_pipeline.py` as a notebook.

### Step 4: Implement the Pipeline

Open `dlt_pipeline.py` and complete the TODO sections:
1. Bronze tables (4 tables)
2. Silver tables (4 tables)
3. Gold tables (5 tables)

### Step 5: Run the Pipeline

Click **Start** to run the pipeline.

## Tips and Tricks

### Import Statement

Use the pipelines module from PySpark:
```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
```

### Defining Tables

Use the `@dp.table()` decorator:
```python
@dp.table(
    name="catalog.schema.table_name",
    comment="Description"
)
def my_table():
    return spark.read.format("...").load("...")
```

### Data Quality Expectations

| Method | Behavior |
|--------|----------|
| `@dp.expect("name", "condition")` | Records violations, keeps rows |
| `@dp.expect_or_drop("name", "condition")` | Removes invalid rows |
| `@dp.expect_or_fail("name", "condition")` | Stops pipeline on violations |

### Reading Tables

- `dp.read("table_name")` - Read static/batch data
- `dp.read_stream("table_name")` - Read streaming data

### Streaming vs Batch

- Use `spark.readStream` for streaming sources → `dp.read_stream()`
- Use `spark.read` for batch sources → `dp.read()`
- Reading from a streaming DLT table propagates streaming

### Data Sources

| Source | Path |
|--------|------|
| IoT Stream | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/` |
| Customers | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/` |
| Products | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/` |
| Stores | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/` |

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE SCHEMA                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ iot_stream   │  │  customers   │  │  products    │  ...     │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SILVER SCHEMA                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ iot_stream   │  │  customers   │  │  products    │  ...     │
│  │ (cleaned)    │  │ (validated)  │  │ (validated)  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         GOLD SCHEMA                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │dim_customers │  │ dim_stores   │  │ dim_products │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────────────────────┐  ┌───────────────────────┐     │
│  │ fact_iot_transactions        │  │ sales_daily_summary   │     │
│  └──────────────────────────────┘  └───────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

## Expected Table Names

| Schema | Table | Description |
|--------|-------|-------------|
| bronze | iot_stream_dlt | Raw IoT transaction stream |
| bronze | customers_dlt | Raw customer data |
| bronze | products_dlt | Raw product data |
| bronze | stores_dlt | Raw store data |
| silver | iot_stream_dlt | Cleaned transaction stream |
| silver | customers_dlt | Deduplicated customers |
| silver | products_dlt | Deduplicated products |
| silver | stores_dlt | Deduplicated stores |
| gold | dim_customers_dlt | Customer dimension |
| gold | dim_stores_dlt | Store dimension |
| gold | dim_products_dlt | Product dimension |
| gold | fact_iot_transactions_dlt | Transaction fact table |
| gold | sales_daily_summary_dlt | Daily sales aggregation |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Pipeline stuck | Check storage permissions and network |
| Schema errors | Verify table names and column references |
| Data quality failures | Check expectation results in DLT UI |
| Missing data | Verify external location access |

## Common Patterns

### Streaming with Autoloader
```python
@dp.table(name="bronze.my_table")
def bronze_my_table():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("abfss://container@storage.dfs.core.windows.net/path/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )
```

### Batch Ingestion
```python
@dp.table(name="bronze.my_batch")
def bronze_my_batch():
    return (
        spark.read
        .format("json")
        .load("abfss://container@storage.dfs.core.windows.net/path/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )
```

### Type Casting
```python
.withColumn("amount", F.col("amount").cast("double"))
.withColumn("date", F.to_date("date"))
.withColumn("id", F.col("id").cast("long"))
```

### Joining Tables
```python
@dp.table(name="gold.fact_table")
def gold_fact_table():
    fact = dp.read_stream("silver.fact")
    dim1 = dp.read("silver.dim1")
    return fact.join(dim1, fact.dim1_id == dim1.id, "left")
```

### Aggregations
```python
.groupBy("date", "category")
.agg(
    F.count("*").alias("count"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("average")
)
```

## Next Steps

After completing this exercise:
1. Add more data quality expectations
2. Implement Slowly Changing Dimensions (SCD) for dimension tables
3. Create dashboards using the gold layer
4. Set up continuous processing mode for real-time data
