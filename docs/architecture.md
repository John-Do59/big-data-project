# Architecture Data Pipeline — Olist (PySpark)

---

## Vue globale

```text
data/raw/          data/bronze/         data/silver/          data/gold/
(CSV bruts)   →   (Parquet bruts)  →   (Parquet nettoyés) →  (KPIs)

     │                   │                    │                   │
     ▼                   ▼                    ▼                   ▼

load_raw.py      to_bronze.py     silver_cleaning.py      pipeline.py
                                      │
                                      ▼
                             quality_checks.py
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
   kpi_sales.py              kpi_delivery.py            kpi_customers.py
                                                              │
                                                              ▼
                                                     kpi_payments.py
```

---

## Couche RAW

Fichiers CSV d'origine, **non modifiés**.

| Fichier                          | Clé primaire       | Nombre de lignes (~) |
| -------------------------------- | ------------------ | -------------------- |
| olist_orders_dataset.csv         | order_id           | 99 441               |
| olist_customers_dataset.csv      | customer_id        | 99 441               |
| olist_order_items_dataset.csv    | order_id + item_id | 112 650              |
| olist_order_payments_dataset.csv | order_id           | 103 886              |
| olist_order_reviews_dataset.csv  | review_id          | 100 000              |
| olist_products_dataset.csv       | product_id         | 32 951               |
| olist_sellers_dataset.csv        | seller_id          | 3 095                |
| olist_geolocation_dataset.csv    | zip_code_prefix    | 1 000 163            |

---

## Couche BRONZE

Données chargées avec Spark puis sauvegardées au format Parquet sans transformation.

### Caractéristiques

* Schémas inférés automatiquement via `inferSchema=True`
* Aucune modification des données
* Réduction du temps de lecture lors des exécutions suivantes
* Conservation d'une copie fidèle des données brutes

### Modules

```text
src/ingestion/load_raw.py
    └── Lecture des CSV avec spark.read.csv()

src/ingestion/to_bronze.py
    └── Écriture des tables dans data/bronze/
```

---

## Couche SILVER

Données nettoyées, typées et fiabilisées.

### Transformations appliquées

| Table       | Traitements                                                                                             |
| ----------- | ------------------------------------------------------------------------------------------------------- |
| orders      | Suppression des doublons, conversion des timestamps, suppression des `order_id` nuls                    |
| customers   | Suppression des doublons, suppression des `customer_id` nuls, remplissage des villes et états manquants |
| order_items | Suppression des doublons, conversion des montants en `double`, remplissage des valeurs manquantes       |
| payments    | Suppression des doublons, conversion de `payment_value`, remplissage des types et échéances             |
| reviews     | Suppression des doublons, conversion de `review_score` en entier, score par défaut = 3                  |
| products    | Suppression des doublons, suppression des `product_id` nuls, remplissage des catégories                 |

### Contrôles qualité

Implémentés dans `quality_checks.py` :

* Détection des valeurs nulles
* Détection des doublons
* Vérification des montants négatifs
* Contrôle de cohérence des dates
* Validation des scores d'avis (1 à 5)

### Modules

```text
src/transformations/silver_cleaning.py

src/transformations/quality_checks.py

Export :
data/silver/<table>.parquet
```

---

## Couche GOLD

Tables analytiques construites à partir de jointures et d'agrégations PySpark.

### Relations principales

```text
orders
│
├── order_items ───► products
│        │
│        └────────► KPIs ventes
│
├── reviews
│        │
│        └────────► Satisfaction client
│
└── payments
         │
         └────────► Répartition des paiements
```

### KPIs produits

| KPI                                    | Module           | Tables utilisées       |
| -------------------------------------- | ---------------- | ---------------------- |
| Chiffre d'affaires total               | kpi_sales.py     | order_items            |
| Chiffre d'affaires mensuel             | kpi_sales.py     | orders + order_items   |
| Panier moyen                           | kpi_sales.py     | order_items            |
| Top catégories                         | kpi_sales.py     | order_items + products |
| Top vendeurs                           | kpi_sales.py     | orders + order_items   |
| Délai moyen de livraison               | kpi_delivery.py  | orders                 |
| Taux de retard                         | kpi_delivery.py  | orders                 |
| Impact des retards sur la satisfaction | kpi_delivery.py  | orders + reviews       |
| Note moyenne client                    | kpi_customers.py | reviews                |
| Répartition des paiements              | kpi_payments.py  | payments               |

### Modules

```text
src/pipeline.py
    └── Orchestration de la couche Gold

src/gold/kpi_*.py
    └── Calcul des KPIs

Export :
data/gold/<kpi>.parquet
```

---

## Point d'entrée

```text
main.py
│
├── Step 1 : load_all_tables()
│            └── RAW
│
├── Step 2 : save_all_bronze()
│            └── BRONZE
│
├── Step 3 : clean_*()
│            └── SILVER
│
├── Step 3b : run_quality_checks()
│             └── Contrôles qualité
│
└── Step 4 : build_gold_pipeline()
              └── GOLD
```

---

## Formats de stockage

| Couche | Format  | Justification                           |
| ------ | ------- | --------------------------------------- |
| RAW    | CSV     | Données sources originales              |
| BRONZE | Parquet | Lecture rapide et typage conservé       |
| SILVER | Parquet | Compression et performances analytiques |
| GOLD   | Parquet | Exploitation BI et reporting            |

---

## Architecture cible

```text
RAW CSV
   ↓
BRONZE PARQUET
   ↓
SILVER PARQUET
   ↓
GOLD KPIs
   ↓
Power BI / Streamlit / Machine Learning
```
