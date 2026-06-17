# Big Data Pipeline — Olist E-Commerce (PySpark)

## Contexte

Ce projet a été réalisé dans le cadre d'une mission Data Engineering / IA visant à exploiter les données d'une entreprise e-commerce brésilienne à partir du dataset public **Olist** disponible sur Kaggle.

Les données sources sont réparties dans plusieurs fichiers CSV. L'objectif est de concevoir une pipeline Big Data locale, reproductible et documentée permettant de transformer des données brutes en indicateurs métier exploitables.

---

## Architecture de la pipeline

```text
data/
├── raw/       # Données sources CSV
├── bronze/    # Données ingestées et stockées en Parquet
├── silver/    # Données nettoyées et fiabilisées
└── gold/      # KPIs et indicateurs métier
```

### Flux de données

```text
CSV (Raw)
    ↓
Bronze
    ↓
Silver
    ↓
Gold (KPIs)
```

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist**

Source : https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

| Fichier CSV                      | Description          |
| -------------------------------- | -------------------- |
| olist_orders_dataset.csv         | Commandes clients    |
| olist_customers_dataset.csv      | Informations clients |
| olist_order_items_dataset.csv    | Articles commandés   |
| olist_order_payments_dataset.csv | Paiements            |
| olist_order_reviews_dataset.csv  | Avis clients         |
| olist_products_dataset.csv       | Produits             |
| olist_sellers_dataset.csv        | Vendeurs             |
| olist_geolocation_dataset.csv    | Géolocalisation      |

---

## Stack technique

| Technologie | Usage                                |
| ----------- | ------------------------------------ |
| PySpark     | Traitement distribué et pipeline ETL |
| Python 3.x  | Orchestration et scripting           |
| Parquet     | Format de stockage colonne optimisé  |
| Pytest      | Tests unitaires                      |
| Git         | Gestion des versions                 |
| Git Flow    | Workflow de développement            |

---

## Structure du projet

```text
project/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── ingestion/
│   │   ├── load_raw.py
│   │   └── to_bronze.py
│   │
│   ├── transformations/
│   │   ├── silver_cleaning.py
│   │   └── quality_checks.py
│   │
│   ├── gold/
│   │   ├── kpi_sales.py
│   │   ├── kpi_delivery.py
│   │   ├── kpi_customers.py
│   │   └── kpi_payments.py
│   │
│   └── pipeline.py
│
├── tests/
│   └── test_pipeline.py
│
├── docs/
│   ├── architecture.md
│   └── data_quality.md
│
├── main.py
├── requirements.txt
└── README.md
```

---

## Installation

### Prérequis

```bash
Python >= 3.8
Java >= 11
PySpark >= 3.x
```

### Installation des dépendances

```bash
pip install -r requirements.txt
```

### Téléchargement des données

Télécharger le dataset Olist depuis Kaggle puis placer les fichiers CSV dans :

```text
data/raw/
```

---

## Exécution de la pipeline

### Lancer le pipeline complet

```bash
python -m src.main
```

### Exécuter les tests

```bash
pytest tests/test_pipeline.py -v
```

---

## KPIs générés (couche Gold)

| KPI                        | Description                                     |
| -------------------------- | ----------------------------------------------- |
| Chiffre d'affaires total   | Somme des ventes réalisées                      |
| Chiffre d'affaires mensuel | Évolution mensuelle du CA                       |
| Panier moyen               | Valeur moyenne des commandes                    |
| Top catégories             | Classement des catégories par CA                |
| Top vendeurs               | Classement des vendeurs par CA                  |
| Délai moyen de livraison   | Différence moyenne entre date estimée et réelle |
| Taux de retard             | Pourcentage de commandes livrées en retard      |
| Note moyenne client        | Moyenne des avis clients                        |
| Impact des retards         | Corrélation entre retard et satisfaction client |
| Répartition des paiements  | Distribution des moyens de paiement             |

---

## Contrôles qualité des données

Des contrôles automatiques sont exécutés lors du passage en couche Silver :

* Détection des valeurs nulles
* Détection des doublons sur les clés métiers
* Vérification des montants négatifs
* Contrôle de cohérence des dates
* Validation des scores d'avis (1 à 5)

Consulter :

```text
src/transformations/quality_checks.py
docs/data_quality.md
```

---

## Tests

La pipeline est couverte par des tests unitaires Pytest permettant de valider :

* L'ingestion des données
* Les transformations Silver
* Les calculs des KPIs Gold
* La qualité des données

```bash
pytest -v
```

---

## Workflow Git

```text
main
└── develop
    ├── feature/ingestion
    ├── feature/silver
    ├── feature/gold-kpis
    └── feature/tests
```

---

## Équipe projet

Projet réalisé par trois apprentis Développeurs Data / IA :

| Membre    | Responsabilités                             |
| --------- | ------------------------------------------- |
| Alexandre | Ingestion RAW → Bronze et workflow Git      |
| Fatima    | Nettoyage Silver et contrôles qualité       |
| Amaury    | Couche Gold, KPIs métier et tests unitaires |

---

## Limites actuelles

* Exécution en mode local (`local[*]`)
* Aucun déploiement sur cluster Spark
* Dataset statique (absence de flux temps réel)
* Analyse NLP des avis clients non implémentée
* Données de géolocalisation non exploitées dans les KPIs

---

## Perspectives d'amélioration

* Déploiement sur cluster Spark
* Orchestration avec Airflow
* Containerisation avec Docker
* Monitoring de la qualité des données
* Analyse NLP des avis clients
* Création de dashboards avec Power BI ou Streamlit
* Intégration de données temps réel via Kafka

---

**Projet réalisé dans le cadre d'une formation Développeur en Intelligence Artificielle (RNCP Niveau 6).**
