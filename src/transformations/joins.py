from pyspark.sql import functions as F


# BASE FACT TABLE


def build_base_dataset(
    orders,
    customers,
    order_items,
    products,
    payments,
    reviews
):

    # STEP 1: orders + customers
    df = orders.join(customers, "customer_id", "left")

    # STEP 2: items (clé centrale du business Olist)
    df = df.join(order_items, "order_id", "left")

    # STEP 3: products (via order_items.product_id)
    df = df.join(products, "product_id", "left")

    # STEP 4: payments
    df = df.join(payments, "order_id", "left")

    # STEP 5: reviews
    df = df.join(reviews, "order_id", "left")

    return df



# FEATURE ENGINEERING (bonus + KPI)


def add_features(df):

    df = df.withColumn(
        "delivery_delay_days",
        F.datediff(
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        )
    )

    df = df.withColumn(
        "is_late",
        F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
    )

    return df