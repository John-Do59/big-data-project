from pyspark.sql import functions as F


def calculate_total_revenue(order_items):
    order_revenue = order_items.groupBy("order_id").agg(F.sum("price").alias("order_value"))
    return order_revenue.agg(F.sum("order_value")).collect()[0][0]


def calculate_avg_basket(order_items):
    order_revenue = order_items.groupBy("order_id").agg(F.sum("price").alias("order_value"))
    return order_revenue.agg(F.avg("order_value")).collect()[0][0]


def calculate_top_categories(order_items, products):
    sales_base = order_items.join(products, "product_id", "left")
    return sales_base.groupBy("product_category_name").agg(F.sum("price").alias("revenue")).orderBy(F.desc("revenue"))


def calculate_top_sellers(orders, order_items):
    return orders.join(order_items, "order_id").groupBy("seller_id").agg(F.sum("price").alias("revenue")).orderBy(F.desc("revenue"))


def calculate_monthly_revenue(order_items, orders):
    """Chiffre d'affaires mensuel"""
    return (
        order_items
        .join(orders.select("order_id", "order_purchase_timestamp"), "order_id", "left")
        .withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .groupBy("month")
        .agg(F.sum("price").alias("monthly_revenue"))
        .orderBy("month")
    )