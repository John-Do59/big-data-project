from pyspark.sql.functions import col, sum, avg, count, round, datediff, when, trunc, desc


def show_kpis(gold):
    sales    = gold["sales"]
    delivery = gold["delivery"]
    payments = gold["payments"]

    #  Ventes 
    print("\n=== 1. CA total ===")
    sales.agg(round(sum("price"), 2).alias("ca_total")).show()

    print("=== 2. CA mensuel ===")
    (sales
        .withColumn("mois", trunc("order_purchase_timestamp", "MM"))
        .groupBy("mois")
        .agg(round(sum("price"), 2).alias("ca_mensuel"))
        .orderBy("mois")
        .show(24))

    print("=== 3. Panier moyen ===")
    (sales
        .groupBy("order_id")
        .agg(sum("price").alias("total"))
        .agg(round(avg("total"), 2).alias("panier_moyen"))
        .show())

    print("=== 4. Top 10 catégories ===")
    (sales
        .groupBy("product_category_name_english")
        .agg(round(sum("price"), 2).alias("ca"))
        .orderBy(desc("ca"))
        .show(10))

    print("=== 5. Top 10 vendeurs ===")
    (sales
        .groupBy("seller_id")
        .agg(round(sum("price"), 2).alias("ca"))
        .orderBy(desc("ca"))
        .show(10))

    print("=== 11. CA par état ===")
    (sales
        .groupBy("customer_state")
        .agg(round(sum("price"), 2).alias("ca"))
        .orderBy(desc("ca"))
        .show(10))

    print("=== 12. Commandes par statut ===")
    (sales
        .groupBy("order_status")
        .agg(count("order_id").alias("nb_commandes"))
        .orderBy(desc("nb_commandes"))
        .show())

    print("=== 15. CA par vendeur et état ===")
    (sales
        .groupBy("seller_id", "seller_state")
        .agg(round(sum("price"), 2).alias("ca"))
        .orderBy(desc("ca"))
        .show(10))

    # Livraison & Satisfaction
    print("=== 6. Délai moyen de livraison ===")
    (delivery
        .filter(col("order_delivered_customer_date").isNotNull())
        .withColumn("delai", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .agg(round(avg("delai"), 1).alias("delai_moyen_jours"))
        .show())

    print("=== 7. Taux de retard ===")
    (delivery
        .filter(col("order_delivered_customer_date").isNotNull())
        .withColumn("en_retard", when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1
        ).otherwise(0))
        .agg(round(avg("en_retard") * 100, 2).alias("taux_retard_pct"))
        .show())

    print("=== 8. Note moyenne client ===")
    delivery.agg(round(avg("review_score"), 2).alias("note_moyenne")).show()

    print("=== 9. Impact des retards sur la satisfaction ===")
    (delivery
        .filter(col("order_delivered_customer_date").isNotNull())
        .withColumn("en_retard", when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"), "retard"
        ).otherwise("a_temps"))
        .groupBy("en_retard")
        .agg(round(avg("review_score"), 2).alias("note_moyenne"))
        .show())

    print("=== 13. Note moyenne par catégorie ===")
    (delivery
        .join(sales.select("order_id", "product_category_name_english").distinct(), "order_id", "left")
        .groupBy("product_category_name_english")
        .agg(round(avg("review_score"), 2).alias("note_moyenne"))
        .orderBy(desc("note_moyenne"))
        .show(10))

    print("=== 14. Délai moyen par état ===")
    (delivery
        .filter(col("order_delivered_customer_date").isNotNull())
        .withColumn("delai", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .groupBy("customer_state")
        .agg(round(avg("delai"), 1).alias("delai_moyen_jours"))
        .orderBy(desc("delai_moyen_jours"))
        .show(10))

    # Paiements
    print("=== 10. Répartition des paiements ===")
    (payments
        .groupBy("payment_type")
        .agg(
            count("order_id").alias("nb_commandes"),
            round(sum("payment_value"), 2).alias("montant_total"),
        )
        .orderBy(desc("nb_commandes"))
        .show())