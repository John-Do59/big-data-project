from pyspark.sql import functions as F
from pyspark.sql.functions import expr


# ORDERS 



def clean_orders(df):
    return (
        df.dropDuplicates(["order_id"])
        # Supprime les lignes sans clé primaire
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        # Conversion des dates
        .withColumn("order_purchase_timestamp",      F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",             F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date",  F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
        # Statut par défaut si manquant
        .fillna({"order_status": "unknown"})
    )


# CUSTOMERS 



def clean_customers(df):
    return (
        df.dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .fillna({
            "customer_city":  "unknown",
            "customer_state": "unknown"
        })
    )


# ORDER ITEMS 



def clean_order_items(df):
    return (
        df.dropDuplicates()
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .withColumn("price",         F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        # Valeurs nulles → 0.0 pour les montants
        .fillna({"price": 0.0, "freight_value": 0.0})
    )


# PAYMENTS 



def clean_payments(df):
    return (
        df.dropDuplicates()
        .filter(F.col("order_id").isNotNull())
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .fillna({
            "payment_value":       0.0,
            "payment_type":        "unknown",
            "payment_installments": 1
        })
    )


# REVIEWS 



def clean_reviews(df):
    return (
        df.dropDuplicates()
        .filter(F.col("order_id").isNotNull())
        .withColumn("review_score", expr("try_cast(review_score as int)"))
        # Score null → médiane 3, commentaire null → chaîne vide
        .fillna({
            "review_score":         3,
            "review_comment_title": "",
            "review_comment_message": ""
        })
    )


# PRODUCTS 



def clean_products(df):
    return (
        df.dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
        .fillna({
            "product_category_name":         "unknown",
            "product_weight_g":              0.0,
            "product_length_cm":             0.0,
            "product_height_cm":             0.0,
            "product_width_cm":              0.0
        })
    )


# UTILITAIRE — suppression des clés nulles 



def remove_null_keys(df, key: str):
    """Supprime les lignes dont la clé primaire/étrangère est nulle."""
    before = df.count()
    cleaned = df.filter(F.col(key).isNotNull())
    after = cleaned.count()
    print(f"    [remove_null_keys] '{key}' : {before - after} lignes supprimées ({before} → {after})")
    return cleaned