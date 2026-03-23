# DLT Pipeline Setup Guide

This guide provides instructions and tips for setting up the Delta Live Tables (DLT) pipeline.

## Prerequisites

- Access to Azure Databricks workspace
- Unity Catalog configured with your catalog
- Storage account access for the landing zone

## Data Sources

| Source | Path |
|--------|------|
| IoT Stream | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/` |
| Customers | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/` |
| Products | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/` |
| Stores | `abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/` |

## Setup Steps

### 1. Create Schemas

Run the following SQL commands in your Databricks workspace:

```sql
CREATE SCHEMA IF NOT EXISTS emanuel_db.bronze;
CREATE SCHEMA IF NOT EXISTS emanuel_db.silver;
CREATE SCHEMA IF NOT EXISTS emanuel_db.gold;
```

### 2. Create the DLT Pipeline

1. Go to **Delta Live Tables** in the sidebar
2. Click **Create Pipeline**
3. Configure the pipeline:
   - **Pipeline Name**: `retail_analytics_dlt`
   - **Product**: `DLT`
   - **Pipeline Mode**: `Triggered` (recommended for learning) or `Continuous`
   - **Target Catalog**: `emanuel_db`
   - **Target Schema**: Leave empty (tables use 3-level namespace)
   - **Storage**: Add your storage path for pipeline artifacts

### 3. Add the Notebook

1. In the pipeline settings, go to **Notebooks**
2. Click **Add notebook** and select `dlt_pipeline.py`

### 4. Start the Pipeline

Click **Start** to run the pipeline in triggered mode, or enable **Continuous** for real-time processing.

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE SCHEMA                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ iot_stream   │  │  customers   │  │  products    │  ...      │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SILVER SCHEMA                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ iot_stream   │  │  customers   │  │  products    │  ...      │
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
│  ┌──────────────────────────────┐  ┌───────────────────────┐    │
│  │ fact_iot_transactions        │  │ sales_daily_summary   │    │
│  └──────────────────────────────┘  └───────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Tips and Tricks

### Data Quality

DLT provides powerful data quality features:

| Method | Behavior |
|--------|----------|
| `@dp.expect()` | Records violations but keeps rows |
| `@dp.expect_or_drop()` | Removes invalid rows (recommended) |
| `@dp.expect_or_fail()` | Stops pipeline on violations |

### Reading Tables

- `dp.read("table_name")` - Read static/batch data
- `dp.read_stream("table_name")` - Read streaming data

### Table Naming

Tables use 3-level namespace: `catalog.schema.table_name`

Example: `emanuel_db.bronze.customers_dlt`

### Streaming vs Batch Detection

DLT automatically detects if a source is streaming or batch based on:
- Using `spark.readStream` = streaming
- Using `spark.read` = batch
- Reading from another DLT table that is streaming = streaming

### Schema Evolution

DLT handles schema evolution automatically. Configure in pipeline settings:
- **Schema evolution mode**: Add new columns automatically
- **Strict schema**: Reject changes

### Troubleshooting

1. **Pipeline stuck**: Check storage permissions and network connectivity
2. **Schema errors**: Verify table names and column references
3. **Data quality failures**: Check expectation results in pipeline UI

### Performance Tips

- Use `dp.read()` instead of `dp.read_stream()` when batch processing is sufficient
- Apply filters early in transformations
- Use appropriate data types (avoid unnecessary casts)
- Monitor pipeline metrics in the DLT UI

## Table Summary

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

## Next Steps

After completing the DLT pipeline, you can:
1. Query gold tables for analytics
2. Create dashboards using the gold layer
3. Add more data quality expectations
4. Implement Slowly Changing Dimensions (SCD) for dimension tables
