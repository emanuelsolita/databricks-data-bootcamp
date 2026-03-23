import os
from pyspark.sql import SparkSession, DataFrame

def is_databricks_environment() -> bool:
    """Check if running in Databricks environment."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_data_path() -> str:
    """Get the appropriate data path based on the environment."""
    if is_databricks_environment():
        # Databricks volume path - adjust this to your actual volume path
        return "/Volumes/catalog/schema/volume/data"
    else:
        # Local relative path
        return "./data"

def get_spark_session(app_name: str = "Ingest Bronze") -> SparkSession:
    """Initialize and return a Spark session."""
    builder = SparkSession.builder.appName(app_name)
    
    # Configure Spark for local environment
    if not is_databricks_environment():
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                         .config("spark.master", "local[*]")
    
    return builder.getOrCreate()

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV file into a Spark DataFrame."""
    return spark.read.option("header", True).csv(path)

def read_json(spark: SparkSession, path: str) -> DataFrame:
    """Read a JSON file into a Spark DataFrame."""
    return spark.read.option("multiline", True).json(path)

def write_to_table(df: DataFrame, table_name: str):
    """Write a DataFrame to a Delta table in overwrite mode."""
    if is_databricks_environment():
        # Use Databricks catalog table
        df.write.mode("overwrite").saveAsTable(table_name)
    else:
        # For local environment, save as parquet files in a local directory
        local_table_path = f"./output/{table_name.replace('.', '/')}"
        print(f"Saving to local path: {local_table_path}")
        df.write.mode("overwrite").parquet(local_table_path)

def main():
    # Get environment-specific data path
    data_path = get_data_path()
    
    # File paths
    sales_path = f"{data_path}/retail_sales.csv"
    store_path = f"{data_path}/store_dim.csv"
    product_path = f"{data_path}/product.json"
    
    print(f"Running in {'Databricks' if is_databricks_environment() else 'Local'} environment")
    print(f"Data path: {data_path}")

    spark = get_spark_session()

    # Read data
    df_sales = read_csv(spark, sales_path)
    df_store = read_csv(spark, store_path)
    df_product = read_json(spark, product_path)

    # Write to bronze tables
    write_to_table(df_sales, "emanuel_db.bronze.retail_sales")
    write_to_table(df_store, "emanuel_db.bronze.store_dim")
    write_to_table(df_product, "emanuel_db.bronze.product_dim")

if __name__ == "__main__":
    main()