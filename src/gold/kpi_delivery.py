from pyspark.sql import functions as F


def calculate_avg_delay(orders):
    logistics_base = orders.withColumn(
        "delivery_delay_days",
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
    )
    return logistics_base.select(F.avg("delivery_delay_days")).collect()[0][0]


def calculate_delay_impact(orders, reviews):
    logistics_base = orders.join(reviews, "order_id", "left").withColumn(
        "delivery_delay_days",
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
    ).withColumn(
        "is_late", F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
    )
    return logistics_base.groupBy("is_late").agg(F.avg("review_score").alias("avg_review")).orderBy("is_late")

def calculate_late_rate(orders):
    """Taux de commandes livrées en retard"""
    delivered = orders.filter(
        F.col("order_delivered_customer_date").isNotNull() &
        F.col("order_estimated_delivery_date").isNotNull()
    ).withColumn(
        "is_late",
        F.when(
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), 1
        ).otherwise(0)
    )
    return delivered.agg(
        (F.sum("is_late") / F.count("*") * 100).alias("late_rate_pct")
    ).collect()[0][0]