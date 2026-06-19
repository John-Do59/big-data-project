# Journal d'incidents — Pipeline Big Data Olist

Ce document recense les principaux problèmes rencontrés pendant la construction de la pipeline, leur diagnostic et leur résolution.

---

## Incident 1 — CSV `order_reviews` mal formaté (colonnes décalées)

**Contexte**
Lors du nettoyage de la table `order_reviews`, le cast de la colonne `review_score` en `integer` provoquait une erreur Spark (`CAST_INVALID_INPUT`), avec des valeurs inattendues comme `2017-07-01 23:08:37` ou du texte libre.

**Diagnostic**
Certains commentaires clients (`review_comment_message`) contiennent des retours à la ligne (`\n`). Sans le bon paramétrage, le parseur CSV de Spark interprète ce retour à la ligne comme la fin d'une ligne du fichier, ce qui décale toutes les colonnes suivantes. Résultat : des dates se retrouvent dans la colonne `review_score`.

**Résolution**
Rechargement du CSV avec l'option `multiLine=True`, qui indique à Spark qu'un champ peut contenir des sauts de ligne :

```python
df_order_reviews = spark.read.csv(
    '../data/raw/olist_order_reviews_dataset.csv',
    header=True,
    escape='"',
    multiLine=True
)
```

**Impact**
Aucune perte de données après correction ; le bronze a été régénéré proprement et le silver recalculé.

---

## Incident 2 — Doublons dans la table `geolocation`

**Contexte**
La table `geolocation` contient environ 1 000 163 lignes pour seulement quelques dizaines de milliers de codes postaux distincts.

**Diagnostic**
Un même `geolocation_zip_code_prefix` est associé à plusieurs latitudes/longitudes légèrement différentes (relevés GPS multiples pour une même zone). Une simple suppression de doublons aurait fait perdre de l'information utile et aurait été arbitraire (quelle ligne garder ?).

**Résolution**
Agrégation par `geolocation_zip_code_prefix` avec calcul de la moyenne des coordonnées :

```python
df_geolocation = df_geolocation.groupBy(
    "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"
).agg(
    avg("geolocation_lat").alias("geolocation_lat"),
    avg("geolocation_lng").alias("geolocation_lng")
)
```

**Impact**
La table passe de ~1 000 163 à ~19 015 lignes, une ligne par code postal, sans perte d'information géographique significative.

---

## Incident 3 — Incohérences de dates dans `orders`

**Contexte**
Lors du contrôle qualité, certaines commandes affichaient une date de livraison (`order_delivered_customer_date`) antérieure à la date d'achat (`order_purchase_timestamp`), ce qui est physiquement impossible.

**Diagnostic**
Erreurs de saisie ou anomalies dans le système source d'origine (hors de notre contrôle, propre au dataset Olist).

**Résolution**
Filtrage des lignes incohérentes tout en conservant les valeurs nulles (commandes en attente de livraison) :

```python
df_orders = df_orders.filter(
    (col("order_delivered_customer_date") >= col("order_purchase_timestamp")) |
    col("order_delivered_customer_date").isNull()
)
```

Le même contrôle a été appliqué entre `order_delivered_customer_date` et `order_delivered_carrier_date`, ainsi qu'entre `order_approved_at` et `order_purchase_timestamp`.

**Impact**
Quelques dizaines de lignes supprimées sur 99 441, impact négligeable sur le volume global.

---

## Incident 4 — Environnement PySpark non fonctionnel (Java Gateway)

**Contexte**
Au lancement du projet, la création de la `SparkSession` échouait avec l'erreur :
`PySparkRuntimeError: [JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number.`

**Diagnostic**
PySpark nécessite une JVM (Java) installée localement pour fonctionner, même si l'on travaille uniquement en Python. Java n'était pas installé sur la machine de développement.

**Résolution**
Installation du JDK :

```bash
sudo apt install default-jdk -y
```

**Impact**
Aucun, une fois Java installé la pipeline a démarré normalement.

---

## Synthèse

| # | Incident | Type | Lignes impactées | Statut |
|---|----------|------|-------------------|--------|
| 1 | Colonnes décalées (order_reviews) | Qualité de données / parsing CSV | 0 (corrigé à la source) |  Résolu |
| 2 | Doublons geolocation | Qualité de données | ~981 000 → agrégées |  Résolu |
| 3 | Dates incohérentes (orders) | Qualité de données | ~25 supprimées |  Résolu |
| 4 | Java non installé | Environnement technique | 0 |  Résolu |