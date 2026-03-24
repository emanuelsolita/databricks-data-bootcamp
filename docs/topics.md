# Databricks Topics by Exercise

## Exercise 0: Introduction to PySpark

- **SparkSession** - Creating and configuring Spark sessions
- **DataFrames** - Creating, reading, and manipulating distributed data
- **Transformations vs Actions** - Lazy evaluation and execution triggers
- **Basic DataFrame Operations** - `select`, `filter`, `groupBy`, `sort`, `orderBy`, `join`
- **Spark SQL** - Running SQL queries on DataFrames and tables
- **Reading/Writing** - Loading CSV, JSON, Parquet; writing to tables and files
- **Streaming Basics** - Introduction to structured streaming with `readStream`

---

## Exercise 1: Data Ingestion (Bronze Layer)

- **Databricks SDK** - Using `WorkspaceClient` to access workspace resources
- **External Locations** - Accessing cloud storage via Databricks external locations
- **Autoloader (cloudFiles)** - Incremental file ingestion with schema inference
- **Structured Streaming** - `spark.readStream` for real-time data ingestion
- **Batch Reading** - `spark.read` for一次性 data loading
- **Writing to Delta Tables** - Saving data to Unity Catalog tables
- **Checkpoints** - Managing streaming state and fault tolerance

---

## Exercise 2: Data Transformation (Silver Layer)

- **Reading from Delta Tables** - `spark.read.table()` for existing tables
- **Data Type Casting** - Converting columns to appropriate types (`cast()`)
- **Date Manipulation** - Converting and parsing dates (`to_date()`)
- **Streaming Writes** - `writeStream` with checkpointing
- **Batch Writes** - `write` with overwrite modes
- **Schema Evolution** - Handling changing schemas with `overwriteSchema`
- **Deduplication** - Removing duplicate records

---

## Exercise 3: Aggregation (Gold Layer)

- **CREATE TABLE (AS SELECT)** - Creating managed tables from queries
- **Table Constraints** - PRIMARY KEY, FOREIGN KEY, NOT NULL
- **ALTER TABLE** - Modifying table schemas
- **Joins in SQL** - INNER, LEFT joins across multiple tables
- **Views** - Creating and replacing virtual tables
- **Dimension & Fact Tables** - Kimball-style data modeling
- **INSERT OVERWRITE** - Replacing table data with transformed data

---

## Exercise 4: Orchestration

- **Databricks Workflows** - Creating and managing data pipelines
- **Jobs & Tasks** - Breaking pipelines into executable units
- **Task Dependencies** - Defining execution order with `depends_on`
- **Job Clusters** - Isolated compute for job execution
- **Scheduling** - Time-based triggers (cron), file arrival, continuous
- **Parameters** - Passing variables between tasks

---

## Exercise 5: Dashboards

- **Databricks Dashboards** - Creating visualization dashboards
- **Data Source Selection** - Connecting to tables and views
- **Visualization Types** - Charts, tables, metrics
- **Dashboard Refresh** - Scheduling automatic data updates
- **Parameters & Filters** - Interactive dashboard controls
