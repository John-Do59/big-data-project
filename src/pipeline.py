from pyspark.sql import functions as F
from src.gold.kpi_sales import (
    calculate_total_revenue,
    calculate_avg_basket,
    calculate_top_categories,
    calculate_top_sellers,
    calculate_monthly_revenue
)
from src.gold.kpi_delivery import (
    calculate_avg_delay,
    calculate_delay_impact,
    calculate_late_rate
)
from src.gold.kpi_customers import calculate_avg_review
from src.gold.kpi_payments import calculate_payment_distribution


# GOLD LAYER — BUSINESS KPI


def build_gold_pipeline(
    orders,
    customers,
    order_items,
    payments,
    reviews,
    products
):
    """
    Construit tous les KPIs métier de la couche Gold.
    Chaque KPI est calculé de manière modulaire et indépendante.
    """

    print("  → Calcul du chiffre d'affaires total...")
    total_revenue = calculate_total_revenue(order_items)

    print("  → Calcul du CA mensuel...")
    monthly_revenue = calculate_monthly_revenue(order_items, orders)

    print("  → Calcul du panier moyen...")
    avg_basket = calculate_avg_basket(order_items)

    print("  → Calcul des top catégories...")
    top_categories = calculate_top_categories(order_items, products)

    print("  → Calcul des top vendeurs...")
    top_sellers = calculate_top_sellers(orders, order_items)

    print("  → Calcul du délai moyen de livraison...")
    avg_delay = calculate_avg_delay(orders)

    print("  → Calcul du taux de retard...")
    late_rate = calculate_late_rate(orders)

    print("  → Calcul de la note moyenne client...")
    avg_review = calculate_avg_review(reviews)

    print("  → Calcul de l'impact des retards sur la satisfaction...")
    delay_impact = calculate_delay_impact(orders, reviews)

    print("  → Calcul de la répartition des paiements...")
    payment_distribution = calculate_payment_distribution(payments)

    return {
        # Scalaires
        "total_revenue": total_revenue,
        "avg_basket": avg_basket,
        "avg_delay": avg_delay,
        "avg_review": avg_review,
        "late_rate": late_rate,
        # DataFrames
        "monthly_revenue": monthly_revenue,
        "top_categories": top_categories,
        "top_sellers": top_sellers,
        "delay_impact": delay_impact,
        "payment_distribution": payment_distribution
    }