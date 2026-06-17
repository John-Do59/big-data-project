# Modèle de données — Olist

## Tables

| Table | Lignes | Colonnes |
|-------|--------|----------|
| orders | 99 441 | 8 |
| order_items | 112 650 | 7 |
| order_payments | 103 886 | 5 |
| order_reviews | 100 000 | 7 |
| customers | 99 441 | 5 |
| products | 32 951 | 9 |
| sellers | 3 095 | 4 |
| category | 71 | 2 |
| geolocation | 1 000 163 | 5 |

## Clés de jointure

| Clé | Tables |
|-----|--------|
| order_id | orders, order_items, order_payments, order_reviews |
| customer_id | orders, customers |
| product_id | order_items, products |
| seller_id | order_items, sellers |
| product_category_name | products, category |
| zip_code_prefix | customers, sellers, geolocation |

## Architecture

- `raw/` → fichiers CSV originaux, non modifiés
- `bronze/` → données chargées avec Spark, format Parquet
- `silver/` → données nettoyées et typées (mercredi)
- `gold/` → indicateurs métier (jeudi)