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
    clean_products
)
from src.pipeline import build_gold_pipeline


# CHEMINS DE SORTIE 

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"


# EXPORT SILVER 

def save_silver(silver: dict):
    """Sauvegarde toutes les tables silver en Parquet."""
    for name, df in silver.items():
        path = f"{SILVER_PATH}/{name}"
        print(f"    Saving silver/{name} → {path}")
        df.write.mode("overwrite").parquet(path)


# EXPORT GOLD 

def save_gold(kpis: dict):
    """Sauvegarde les KPIs DataFrames de la couche Gold en Parquet."""
    dataframe_kpis = [
        "monthly_revenue",
        "top_categories",
        "top_sellers",
        "delay_impact",
        "payment_distribution"
    ]
    for name in dataframe_kpis:
        if name in kpis and kpis[name] is not None:
            path = f"{GOLD_PATH}/{name}"
            print(f"    Saving gold/{name} → {path}")
            kpis[name].write.mode("overwrite").parquet(path)


# AFFICHAGE DES KPIs 

def print_kpis(kpis: dict):
    """Affiche tous les KPIs dans la console."""

    print("\n" + "=" * 50)
    print("  KPI RESULTS — OLIST E-COMMERCE")
    print("=" * 50)

    print(f"\n  Chiffre d'affaires total : R$ {kpis['total_revenue']:,.2f}")
    print(f"  Panier moyen             : R$ {kpis['avg_basket']:,.2f}")
    print(f"  Délai moyen de livraison : {kpis['avg_delay']:,.1f} jours")
    print(f"  Note moyenne client      : {kpis['avg_review']:,.2f} / 5")
    print(f"  Taux de retard           : {kpis['late_rate']:,.1f} %")

    print("\n--- CA Mensuel ---")
    kpis["monthly_revenue"].show(24, truncate=False)

    print("\n--- Top Catégories ---")
    kpis["top_categories"].show(10, truncate=False)

    print("\n--- Top Vendeurs ---")
    kpis["top_sellers"].show(10, truncate=False)

    print("\n--- Impact des retards sur la satisfaction ---")
    kpis["delay_impact"].show(truncate=False)

    print("\n--- Répartition des paiements ---")
    kpis["payment_distribution"].show(truncate=False)



# MAIN 

def main():
    spark = SparkSession.builder \
        .appName("Olist-Pipeline") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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
        "orders":      clean_orders(raw_dfs["orders"]),
        "customers":   clean_customers(raw_dfs["customers"]),
        "order_items": clean_order_items(raw_dfs["order_items"]),
        "payments":    clean_payments(raw_dfs["payments"]),
        "reviews":     clean_reviews(raw_dfs["reviews"]),
        "products":    clean_products(raw_dfs["products"])
    }

    print("\n  Saving Silver layer as Parquet...")
    save_silver(silver)

    # STEP 3b : QUALITY CHECKS 
    print("\nStep 3b: Running quality checks...")
    run_quality_checks(silver)

    # STEP 4 : GOLD 
    print("\nStep 4: Building Gold layer KPIs...")
    kpis = build_gold_pipeline(
        orders=silver["orders"],
        customers=silver["customers"],
        order_items=silver["order_items"],
        payments=silver["payments"],
        reviews=silver["reviews"],
        products=silver["products"]
    )

    print("\n  Saving Gold layer as Parquet...")
    save_gold(kpis)

    #  AFFICHAGE 
    print_kpis(kpis)

    print("\n=== Pipeline terminée avec succès ===\n")
    spark.stop()


if __name__ == "__main__":
    main()