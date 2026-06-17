# src/ingestion/load_raw.py

from pyspark.sql import SparkSession

RAW_PATH = "data/raw"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Olist-Raw-Ingestion")
        .master("local[*]")
        .getOrCreate()
    )

def load_csv(spark, filename):
    path = f"{RAW_PATH}/{filename}"
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def load_all_tables(spark):
    tables = {
        "orders": "olist_orders_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "product_category_translation": "product_category_name_translation.csv",
    }

    dataframes = {}

    for name, file in tables.items():
        df = load_csv(spark, file)
        print(f"Loaded {name}")
        df.printSchema()
        print(f"Rows: {df.count()}")
        dataframes[name] = df

    return dataframes


if __name__ == "__main__":
    spark = create_spark_session()
    dfs = load_all_tables(spark)
    spark.stop()