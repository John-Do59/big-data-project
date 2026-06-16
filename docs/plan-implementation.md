#  Plan d’implémentation — Pipeline Big Data Olist (PySpark)

##  Objectif global

Construire une pipeline Big Data locale avec PySpark permettant de transformer les données brutes du dataset Olist en tables analytiques exploitables selon une architecture :

RAW → BRONZE → SILVER → GOLD

---

#  1. Phase 0 — Setup du projet

##  Objectif
Préparer l’environnement de travail et la structure du projet.

## Actions
- création repo Git
- mise en place branches (main / develop / feature/*)
- création environnement Python (.venv)
- installation PySpark
- ajout requirements.txt
- ajout .gitignore
- création structure dossier

## Livrable
- projet prêt à coder
- environnement fonctionnel

---

#  2. Phase 1 — Ingestion RAW → BRONZE

##  Objectif
Charger les fichiers CSV dans Spark.

## Actions
- création SparkSession
- lecture des 8 fichiers CSV Olist
- validation des schémas
- vérification des nulls et types
- comptage des lignes

## Sortie
- DataFrames Spark chargés

---

##  BRONZE layer

- export des DataFrames en Parquet
- aucune transformation métier
- format standardisé Spark

## Exemple

RAW CSV → Spark DataFrame → Parquet (bronze)

---

#  3. Phase 2 — SILVER (cleaning & quality)

##  Objectif
Nettoyer et fiabiliser les données.

## Actions

###  Nettoyage
- conversion des dates
- cast des types numériques
- suppression doublons
- gestion des valeurs nulles

###  Qualité
- détection incohérences simples
- vérification clés primaires
- analyse des distributions

## Sortie
- tables silver fiables et typées

---

#  4. Phase 3 — JOINTURES

##  Objectif
Construire un modèle de données exploitable.

## Tables principales

- orders
- customers
- order_items
- payments
- products
- sellers
- reviews

## Jointures clés

- orders ↔ customers
- orders ↔ order_items
- order_items ↔ products
- order_items ↔ sellers
- orders ↔ payments
- orders ↔ reviews

## Sortie
- dataset unifié enrichi

---

#  5. Phase 4 — GOLD (KPIs métier)

##  Objectif
Créer les indicateurs business.

---

##  Business KPIs
- chiffre d’affaires total
- chiffre d’affaires mensuel
- panier moyen

---

##  Produits
- top catégories
- top vendeurs

---

##  Logistique
- délai moyen de livraison
- taux de retard

---

##  Satisfaction client
- note moyenne
- impact retard sur satisfaction

---

##  Paiements
- répartition des moyens de paiement

---

## Sortie
- tables analytiques prêtes pour reporting

---

#  6. Phase 5 — Export & stockage

##  Objectif
Rendre les données exploitables

## Actions
- export Parquet des tables silver
- export Parquet des tables gold
- organisation dans data lake

## Structure finale

```text id="storage1"
data/
├── raw/
├── bronze/
├── silver/
└── gold/