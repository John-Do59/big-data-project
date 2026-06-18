from pyspark.sql import functions as F
from pyspark.sql import DataFrame


# 1. VÉRIFICATION DES VALEURS NULLES 


def check_nulls(df: DataFrame, table_name: str) -> None:
    """Affiche le nombre de valeurs nulles par colonne."""
    print(f"\n  [check_nulls] Table : {table_name}")
    total = df.count()
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            pct = (null_count / total) * 100
            print(f"    ⚠ {col_name} : {null_count} nulls ({pct:.1f}%)")
    print(f"    ✓ Vérification nulls terminée ({total} lignes)")


# 2. VÉRIFICATION DES DOUBLONS 


def check_duplicates(df: DataFrame, key: str, table_name: str) -> None:
    """Détecte les doublons sur une clé primaire."""
    print(f"\n  [check_duplicates] Table : {table_name} — clé : {key}")
    total = df.count()
    distinct = df.select(key).distinct().count()
    dupes = total - distinct
    if dupes > 0:
        print(f"    ⚠ {dupes} doublons détectés sur '{key}'")
    else:
        print(f"    ✓ Aucun doublon sur '{key}'")


# 3. VÉRIFICATION DES MONTANTS NÉGATIFS


def check_negative_values(df: DataFrame, col_name: str, table_name: str) -> None:
    """Détecte les valeurs négatives sur une colonne numérique."""
    print(f"\n  [check_negative_values] Table : {table_name} — colonne : {col_name}")
    neg_count = df.filter(F.col(col_name) < 0).count()
    if neg_count > 0:
        print(f"    ⚠ {neg_count} valeurs négatives détectées dans '{col_name}'")
    else:
        print(f"    ✓ Aucune valeur négative dans '{col_name}'")


# 4. VÉRIFICATION DE COHÉRENCE DES DATES 


def check_date_coherence(df: DataFrame, table_name: str) -> None:
    """
    Vérifie que la date de livraison réelle
    est postérieure à la date d'achat.
    """
    print(f"\n  [check_date_coherence] Table : {table_name}")
    incoherent = df.filter(
        F.col("order_delivered_customer_date").isNotNull() &
        F.col("order_purchase_timestamp").isNotNull() &
        (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"))
    ).count()
    if incoherent > 0:
        print(f"    ⚠ {incoherent} commandes livrées AVANT la date d'achat")
    else:
        print(f"    ✓ Cohérence des dates OK")


# 5. VÉRIFICATION DES REVIEW SCORES 


def check_review_scores(df: DataFrame) -> None:
    """Vérifie que les scores sont bien compris entre 1 et 5."""
    print(f"\n  [check_review_scores] Table : reviews")
    invalid = df.filter(
        F.col("review_score").isNotNull() &
        ((F.col("review_score") < 1) | (F.col("review_score") > 5))
    ).count()
    if invalid > 0:
        print(f"    ⚠ {invalid} scores invalides (hors 1-5)")
    else:
        print(f"    ✓ Tous les scores sont valides (1-5)")



# 6. RAPPORT QUALITÉ GLOBAL 


def run_quality_checks(silver: dict) -> None:
    """
    Lance tous les contrôles qualité sur les tables silver.
    À appeler dans main.py après le nettoyage silver.
    """
    print("\n" + "=" * 50)
    print("  QUALITY CHECKS — SILVER LAYER")
    print("=" * 50)

    # Nulls
    check_nulls(silver["orders"],      "orders")
    check_nulls(silver["customers"],   "customers")
    check_nulls(silver["order_items"], "order_items")
    check_nulls(silver["payments"],    "payments")
    check_nulls(silver["reviews"],     "reviews")
    check_nulls(silver["products"],    "products")

    # Doublons
    check_duplicates(silver["orders"],    "order_id",    "orders")
    check_duplicates(silver["customers"], "customer_id", "customers")
    check_duplicates(silver["products"],  "product_id",  "products")

    # Montants négatifs
    check_negative_values(silver["order_items"], "price",         "order_items")
    check_negative_values(silver["order_items"], "freight_value", "order_items")
    check_negative_values(silver["payments"],    "payment_value", "payments")

    # Cohérence des dates
    check_date_coherence(silver["orders"], "orders")

    # Review scores
    check_review_scores(silver["reviews"])

    print("\n" + "=" * 50)
    print("  Quality checks terminés")
    print("=" * 50 + "\n")