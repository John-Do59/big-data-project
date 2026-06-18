from pyspark.sql import SparkSession

from src.ingestion.load_raw import load_all_tables
from src.ingestion.to_bronze import save_all_bronze

from src.transformations.quality_checks import run_quality_checks
from src.transformations.silver_cleaning import (
    clean_orders,
    clean_customers,
    clean_order_items,
    clean_payments,
    clean_reviews,
    clean_products,
    clean_geolocation,
    clean_sellers,
    clean_category,
)
from src.transformations.joins import build_and_save_joins
from src.gold import show_kpis

SILVER_PATH = "data/silver"
GOLD_PATH   = "data/gold"


def save_silver(silver: dict):
    """Sauvegarde toutes les tables Silver en Parquet."""
    for name, df in silver.items():
        path = f"{SILVER_PATH}/{name}"
        print(f"    Saving silver/{name} → {path}")
        df.write.mode("overwrite").parquet(path)


def run_pipeline(spark):
    print("\n=== Olist Big Data Pipeline ===\n")

    # STEP 1 : RAW
    print("Step 1: Loading raw data...")
    raw_dfs = load_all_tables(spark)

    # STEP 2 : BRONZE
    print("\nStep 2: Saving to Bronze layer...")
    save_all_bronze(raw_dfs)

    # STEP 3 : SILVER
    print("\nStep 3: Cleaning data for Silver layer...")
    silver = {
        "orders":                       clean_orders(raw_dfs["orders"]),
        "customers":                    clean_customers(raw_dfs["customers"]),
        "order_items":                  clean_order_items(raw_dfs["order_items"]),
        "payments":                     clean_payments(raw_dfs["payments"]),
        "reviews":                      clean_reviews(raw_dfs["reviews"]),
        "products":                     clean_products(raw_dfs["products"]),
        "sellers":                      clean_sellers(raw_dfs["sellers"]),
        "geolocation":                  clean_geolocation(raw_dfs["geolocation"]),
        "product_category_translation": clean_category(raw_dfs["product_category_translation"]),
    }

    print("\n  Saving Silver layer as Parquet...")
    save_silver(silver)

    # STEP 3b : QUALITY CHECKS
    print("\nStep 3b: Running quality checks...")
    run_quality_checks(silver)

    # STEP 4 : GOLD
    print("\nStep 4: Building joins and Gold layer...")
    gold = build_and_save_joins(spark)

    # STEP 5 : KPIs
    print("\nStep 5: Computing KPIs...")
    show_kpis(gold)