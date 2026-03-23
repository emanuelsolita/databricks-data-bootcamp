import dlt
from pyspark.sql import functions as F

# BRONZE SCHEMA - Data Ingestion


@dlt.table(
    name="iot_stream_dlt", comment="Raw IoT transaction stream from landing zone"
)
@dlt.expect_or_drop("valid_transaction_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
def bronze_iot_stream_dlt():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/iot_stream/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )


@dlt.table(name="customers_dlt", comment="Raw customer data from landing zone")
@dlt.expect_or_drop("valid_customer_id", "id IS NOT NULL")
def bronze_customers_dlt():
    return (
        spark.read.format("json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/customers/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )


@dlt.table(name="products_dlt", comment="Raw product data from landing zone")
@dlt.expect_or_drop("valid_product_id", "id IS NOT NULL")
def bronze_products_dlt():
    return (
        spark.read.format("json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/products/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )


@dlt.table(name="stores_dlt", comment="Raw store data from landing zone")
@dlt.expect_or_drop("valid_store_id", "id IS NOT NULL")
def bronze_stores_dlt():
    return (
        spark.read.format("json")
        .load("abfss://landing@landingneusa.dfs.core.windows.net/bootcamp/stores/")
        .withColumn("etl_timestamp", F.current_timestamp())
    )


# SILVER SCHEMA - Data Transformation


@dlt.table(name="silver_iot_stream_dlt", comment="Cleaned and typed transaction stream")
@dlt.expect_or_drop("valid_transaction_amount", "transaction_amount IS NOT NULL")
@dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
def silver_iot_stream_dlt():
    return (
        dlt.read_stream("bronze_iot_stream_dlt")
        .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
        .withColumn("transaction_date", F.to_date("transaction_date"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("store_id", F.col("store_id").cast("long"))
        .withColumn("receipt_id", F.col("receipt_id").cast("long"))
        .drop("_rescued_data")
    )


@dlt.table(name="silver_customers_dlt", comment="Deduplicated and typed customer data")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect("valid_email_format", "customer_email LIKE '%@%' OR customer_email IS NULL")
@dlt.expect("reasonable_age", "customer_age > 0 AND customer_age < 150")
def silver_customers_dlt():
    return (
        dlt.read("bronze_customers_dlt")
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("date"))
    )


@dlt.table(name="silver_products_dlt", comment="Deduplicated and typed product data")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_products_dlt():
    return (
        dlt.read("bronze_products_dlt")
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("date"))
    )


@dlt.table(name="silver_stores_dlt", comment="Deduplicated and typed store data")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_stores_dlt():
    return (
        dlt.read("bronze_stores_dlt")
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("date"))
    )


# GOLD SCHEMA - Dimension and Fact Tables


@dlt.table(name="dim_customers_dlt", comment="Customer dimension table")
@dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
def gold_dim_customers_dlt():
    return dlt.read("silver_customers_dlt").select(
        "id", "customer_name", "customer_email", "customer_age", "etl_timestamp"
    )


@dlt.table(name="dim_stores_dlt", comment="Store dimension table")
@dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
def gold_dim_stores_dlt():
    return dlt.read("silver_stores_dlt").select(
        "id", "store_name", "store_city", "store_country", "etl_timestamp"
    )


@dlt.table(name="dim_products_dlt", comment="Product dimension table")
@dlt.expect_or_drop("valid_pk", "id IS NOT NULL")
def gold_dim_products_dlt():
    return dlt.read("silver_products_dlt").select(
        "id", "product_name", "product_category", "product_price", "etl_timestamp"
    )


@dlt.table(
    name="fact_iot_transactions_dlt",
    comment="IoT transaction fact table with dimension joins",
)
@dlt.expect_or_drop(
    "valid_transaction",
    "transaction_amount IS NOT NULL AND transaction_date IS NOT NULL",
)
def gold_fact_iot_transactions_dlt():
    iot = dlt.read_stream("silver_iot_stream_dlt")
    customers = dlt.read("silver_customers_dlt")
    products = dlt.read("silver_products_dlt")
    stores = dlt.read("silver_stores_dlt")

    return (
        iot.join(customers, iot.customer_id == customers.id, "left")
        .join(products, iot.product_id == products.id, "left")
        .join(stores, iot.store_id == stores.id, "left")
        .select(
            iot.id,
            iot.transaction_date,
            iot.transaction_amount,
            iot.customer_id,
            iot.product_id,
            iot.store_id,
            iot.etl_timestamp,
            customers.customer_name,
            products.product_name,
            products.product_category,
            stores.store_name,
            stores.store_city,
        )
    )


@dlt.table(name="sales_daily_summary_dlt", comment="Daily sales summary for dashboards")
def gold_sales_daily_summary_dlt():
    return (
        dlt.read("fact_iot_transactions_dlt")
        .groupBy("transaction_date", "store_name", "product_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("transaction_amount").alias("total_revenue"),
            F.avg("transaction_amount").alias("avg_transaction"),
        )
    )
