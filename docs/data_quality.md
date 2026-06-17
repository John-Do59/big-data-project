# Rapport qualité des données — Olist Pipeline

---

## Périmètre

Ce document décrit les problèmes de qualité identifiés sur le dataset Olist
lors du passage de la couche RAW à la couche SILVER, ainsi que les décisions
de traitement appliquées.

---

## 1. Valeurs nulles

### orders

| Colonne                          | Nulls détectés | Traitement appliqué              |
|----------------------------------|----------------|----------------------------------|
| order_approved_at                | ~160           | Conservé null (approbation rare) |
| order_delivered_carrier_date     | ~1800          | Conservé null (non expédiées)    |
| order_delivered_customer_date    | ~2965          | Conservé null (non livrées)      |
| order_estimated_delivery_date    | 0              | Aucun                            |
| order_status                     | 0              | fillna("unknown") par sécurité   |

### order_items

| Colonne        | Nulls détectés | Traitement appliqué |
|----------------|----------------|---------------------|
| price          | 0              | fillna(0.0)         |
| freight_value  | 0              | fillna(0.0)         |

### reviews

| Colonne                  | Nulls détectés | Traitement appliqué              |
|--------------------------|----------------|----------------------------------|
| review_score             | ~0             | fillna(3) — valeur médiane       |
| review_comment_title     | ~58 247        | fillna("") — champ optionnel     |
| review_comment_message   | ~51 956        | fillna("") — champ optionnel     |

### products

| Colonne                   | Nulls détectés | Traitement appliqué              |
|---------------------------|----------------|----------------------------------|
| product_category_name     | ~610           | fillna("unknown")                |
| product_weight_g          | ~2             | fillna(0.0)                      |
| product_length_cm         | ~2             | fillna(0.0)                      |

### customers

| Colonne          | Nulls détectés | Traitement appliqué |
|------------------|----------------|---------------------|
| customer_city    | 0              | fillna("unknown")   |
| customer_state   | 0              | fillna("unknown")   |

---

## 2. Doublons

| Table        | Clé contrôlée  | Doublons détectés | Traitement        |
|--------------|----------------|-------------------|-------------------|
| orders       | order_id       | 0                 | dropDuplicates()  |
| customers    | customer_id    | 0                 | dropDuplicates()  |
| order_items  | —              | 0                 | dropDuplicates()  |
| payments     | —              | 0                 | dropDuplicates()  |
| reviews      | —              | 0                 | dropDuplicates()  |
| products     | product_id     | 0                 | dropDuplicates()  |

---

## 3. Incohérences détectées

### Dates incohérentes
Commandes où `order_delivered_customer_date` < `order_purchase_timestamp` :
contrôle effectué via `check_date_coherence()` — aucune incohérence détectée.

### Montants négatifs
Contrôle effectué sur `price`, `freight_value`, `payment_value` :
aucune valeur négative détectée.

### Scores d'avis invalides
Contrôle effectué sur `review_score` (valeurs attendues : 1 à 5) :
aucune valeur hors plage détectée.

---

## 4. Décisions de nettoyage justifiées

| Décision                          | Justification métier                                          |
|-----------------------------------|---------------------------------------------------------------|
| review_score null → 3             | Valeur médiane neutre, ne fausse pas la moyenne               |
| category null → "unknown"         | Permet de conserver la ligne dans les agrégations             |
| dates livraison null conservées   | Normal pour commandes non encore livrées                      |
| price null → 0.0                  | Evite les erreurs de calcul, signalé dans le journal          |
| commentaires null → ""            | Champ optionnel, absence normale dans le dataset              |

---

## 5. Couverture des contrôles qualité

Tous les contrôles sont automatisés dans `src/transformations/quality_checks.py`
et s'exécutent à chaque lancement de la pipeline après la couche Silver.

| Contrôle                    | Fonction                    | Tables concernées          |
|-----------------------------|-----------------------------|----------------------------|
| Valeurs nulles par colonne  | check_nulls()               | Toutes                     |
| Doublons sur clé primaire   | check_duplicates()          | orders, customers, products|
| Montants négatifs           | check_negative_values()     | order_items, payments      |
| Cohérence des dates         | check_date_coherence()      | orders                     |
| Scores d'avis valides       | check_review_scores()       | reviews                    |