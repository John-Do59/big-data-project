from pyspark.sql import functions as F
from pyspark.sql.functions import expr, to_timestamp, col


# ORDERS 



def clean_orders(df):
    return (
        df.dropDuplicates(["order_id"])
        .dropna(subset=["order_id", "customer_id"])
        .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
        .fillna({"order_status": "unknown"})
        # Contrôles métier
        .filter(
            (col("order_delivered_customer_date") >= col("order_purchase_timestamp")) |
            col("order_delivered_customer_date").isNull()
        )
        .filter(
            (col("order_delivered_customer_date") >= col("order_delivered_carrier_date")) |
            col("order_delivered_customer_date").isNull()
        )
        .filter(
            (col("order_approved_at") >= col("order_purchase_timestamp")) |
            col("order_approved_at").isNull()
        )
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
        .dropna(subset=["order_id", "product_id", "seller_id"])
        .withColumn("order_item_id", col("order_item_id").cast("integer"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("freight_value", col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
        .fillna({
            "price": 0.0,
            "freight_value": 0.0
        })
    )

# PAYMENTS 



def clean_payments(df):
    return (
        df.dropDuplicates()
        .dropna(subset=["order_id"])
        .withColumn("payment_sequential",
                    col("payment_sequential").cast("integer"))
        .withColumn("payment_installments",
                    col("payment_installments").cast("integer"))
        .withColumn("payment_value",
                    col("payment_value").cast("double"))
        .fillna({
            "payment_type": "unknown",
            "payment_installments": 1,
            "payment_value": 0.0
        })
    )

# REVIEWS 


def clean_reviews(df):
    return (
        df.dropDuplicates(["review_id", "order_id"])
        .dropna(subset=["review_id", "order_id"])
        .withColumn("review_score",
                    expr("try_cast(review_score as int)"))
        .withColumn("review_creation_date",
                    to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp",
                    to_timestamp("review_answer_timestamp"))
        .fillna({
            "review_score": 3,
            "review_comment_title": "",
            "review_comment_message": ""
        })
    )

# PRODUCTS 

def clean_products(df):
    return (
        df.dropDuplicates(["product_id"])
        .dropna(subset=["product_id"])
        .withColumn("product_name_lenght",
                    col("product_name_lenght").cast("integer"))
        .withColumn("product_description_lenght",
                    col("product_description_lenght").cast("integer"))
        .withColumn("product_photos_qty",
                    col("product_photos_qty").cast("integer"))
        .withColumn("product_weight_g",
                    col("product_weight_g").cast("integer"))
        .withColumn("product_length_cm",
                    col("product_length_cm").cast("integer"))
        .withColumn("product_height_cm",
                    col("product_height_cm").cast("integer"))
        .withColumn("product_width_cm",
                    col("product_width_cm").cast("integer"))
        .fillna({
            "product_category_name": "unknown",
            "product_weight_g": 0,
            "product_length_cm": 0,
            "product_height_cm": 0,
            "product_width_cm": 0
        })
    )


# GEOLOCATION 


def clean_geolocation(df):
    return (
        df.withColumn("geolocation_lat",
                      col("geolocation_lat").cast("double"))
        .withColumn("geolocation_lng",
                    col("geolocation_lng").cast("double"))
        .dropna(subset=["geolocation_zip_code_prefix"])
        .groupBy(
            "geolocation_zip_code_prefix",
            "geolocation_city",
            "geolocation_state"
        )
        .agg(
            avg("geolocation_lat").alias("geolocation_lat"),
            avg("geolocation_lng").alias("geolocation_lng")
        )
    )

# SELLERS


def clean_sellers(df):
    return (
        df.dropDuplicates(["seller_id"])
        .dropna(subset=["seller_id"])
        .fillna({
            "seller_city": "unknown",
            "seller_state": "unknown"
        })
    )