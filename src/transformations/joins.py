from pyspark.sql.functions import col


def build_and_save_joins(spark):
    #  Lecture silver 
    df_orders         = spark.read.parquet("data/silver/orders")
    df_order_items    = spark.read.parquet("data/silver/order_items")
    df_order_payments = spark.read.parquet("data/silver/payments")
    df_order_reviews  = spark.read.parquet("data/silver/reviews")
    df_customers      = spark.read.parquet("data/silver/customers")
    df_products       = spark.read.parquet("data/silver/products")
    df_sellers        = spark.read.parquet("data/silver/sellers")
    df_category       = spark.read.parquet("data/silver/product_category_translation")
    df_geolocation    = spark.read.parquet("data/silver/geolocation")

    #  Base commune / silver
    df_items_base = (
        df_order_items
        .join(df_orders,    "order_id")
        .join(df_products,  "product_id")
        .join(df_sellers,   "seller_id")
        .join(df_category,  "product_category_name", "left")
        .join(df_customers, "customer_id")
    )
    df_items_base.write.mode("overwrite").parquet("../data/silver/items_base")

    #  gold_sales 
    df_gold_sales = (
        df_items_base
        .join(df_customers.select("customer_id", "customer_state").alias("cust"), "customer_id")
        .select(
            "order_id", "order_item_id", "product_id", "seller_id", "seller_state",
            "price", "freight_value", "order_status",
            "order_purchase_timestamp", "order_delivered_customer_date",
            "order_estimated_delivery_date", "product_category_name_english",
            "customer_id", col("cust.customer_state").alias("customer_state"),
        )
    )
    df_gold_sales.write.mode("overwrite").parquet("../data/gold/gold_sales")

    #  gold_delivery_satisfaction
    df_gold_delivery_satisfaction = (
        df_items_base
        .join(df_customers.select("customer_id", "customer_state").alias("cust"), "customer_id")
        .join(df_order_reviews.select("order_id", "review_score", "review_comment_message"), "order_id", "left")
        .select(
            "order_id", "seller_id", col("cust.customer_state").alias("customer_state"),
            "order_purchase_timestamp", "order_delivered_customer_date",
            "order_estimated_delivery_date", "order_status",
            "review_score", "review_comment_message",
        )
        .dropDuplicates(["order_id", "seller_id"])
    )
    df_gold_delivery_satisfaction.write.mode("overwrite").parquet("../data/gold/gold_delivery_satisfaction")

    #  gold_payments
    df_gold_payments = (
        df_order_payments
        .join(df_orders.select("order_id", "order_status", "customer_id", "order_purchase_timestamp"), "order_id", "left")
        .join(df_customers.select("customer_id", "customer_state"), "customer_id", "left")
        .select(
            "order_id", "payment_type", "payment_value",
            "payment_installments", "payment_sequential",
            "order_status", "order_purchase_timestamp", "customer_state",
        )
    )
    df_gold_payments.write.mode("overwrite").parquet("../data/gold/gold_payments")

    #  gold_geo
    df_cust = df_customers.select(
        "customer_id", "customer_state", "customer_city", "customer_zip_code_prefix"
    ).alias("cust")

    df_gold_geo = (
        df_items_base
        .join(df_cust, "customer_id")
        .join(df_geolocation.alias("geo"),
              col("cust.customer_zip_code_prefix") == col("geo.geolocation_zip_code_prefix"), "left")
        .select(
            "order_id", "price", "freight_value",
            "order_purchase_timestamp", "product_category_name_english",
            col("cust.customer_state").alias("customer_state"),
            col("cust.customer_city").alias("customer_city"),
            "seller_state", "seller_city",
            col("geo.geolocation_lat").alias("geolocation_lat"),
            col("geo.geolocation_lng").alias("geolocation_lng"),
        )
    )
    df_gold_geo.write.mode("overwrite").parquet("../data/gold/gold_geo")

    print("  Jointures terminées — items_base (silver) et tables gold écrites.")

    # Retourne les DataFrames 
    return {
        "sales":    df_gold_sales,
        "delivery": df_gold_delivery_satisfaction,
        "payments": df_gold_payments,
        "geo":      df_gold_geo,
    }