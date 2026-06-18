from pyspark.sql import functions as F


def calculate_revenue_by_state(order_items, orders, customers):
    """
    CA par état brésilien (customer_state depuis la table customers).
    Jointure : order_items → orders → customers
    """
    return (
        order_items
        .join(orders.select("order_id", "customer_id"), "order_id", "left")
        .join(customers.select("customer_id", "customer_state"), "customer_id", "left")
        .groupBy("customer_state")
        .agg(F.sum("price").alias("revenue"))
        .orderBy(F.desc("revenue"))
    )


def calculate_orders_by_state(orders, customers):
    """
    Nombre de commandes par état brésilien.
    Utile pour distinguer volume vs chiffre d'affaires.
    """
    return (
        orders.select("order_id", "customer_id")
        .join(customers.select("customer_id", "customer_state"), "customer_id", "left")
        .groupBy("customer_state")
        .agg(F.count("order_id").alias("nb_orders"))
        .orderBy(F.desc("nb_orders"))
    )


def calculate_avg_basket_by_state(order_items, orders, customers):
    """
    Panier moyen par état brésilien.
    Permet de comparer le comportement d'achat selon la région.
    """
    return (
        order_items
        .join(orders.select("order_id", "customer_id"), "order_id", "left")
        .join(customers.select("customer_id", "customer_state"), "customer_id", "left")
        .groupBy("order_id", "customer_state")
        .agg(F.sum("price").alias("order_value"))
        .groupBy("customer_state")
        .agg(F.avg("order_value").alias("avg_basket"))
        .orderBy(F.desc("avg_basket"))
    )