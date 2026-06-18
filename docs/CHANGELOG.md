# CHANGELOG — Pipeline Olist Big Data

Toutes les modifications et améliorations apportées au projet.

---

## [2.0] — 2026-06-17 — Finalisation complète

### ✨ Nouvelles fonctionnalités
- Création de `src/main.py` : point d'entrée unique pour exécuter la pipeline complète
- Implémentation complète des modules de la couche Gold :
  - `kpi_sales.py` : CA total, CA mensuel, panier moyen, top catégories, top vendeurs
  - `kpi_delivery.py` : délai moyen, taux de retard, impact des retards sur la satisfaction
  - `kpi_customers.py` : note moyenne client
  - `kpi_payments.py` : répartition des paiements
- Implémentation complète de `quality_checks.py` avec règles métier

### 🔧 Corrections
- **Fan-out data Gold** : Suppression de la super-table consolidée, chaque KPI utilise uniquement les jointures nécessaires
- **Délai de livraison négatif** : Filtre sur les commandes uniquement "delivered" avec dates valides
- **Review score invalid** : Remplacement de `.cast(int)` par `try_cast()` + fillna(3) pour valeurs invalides
- **Commande d'exécution README** : Correction de `python main.py` → `python -m src.main`

### 📝 Documentation
- Création de `docs/improvements-summary.md`
- Mise à jour de `docs/data_quality.md` avec les vrais chiffres de doublons
- Finalisation de `README.md` (business-focused)

---

## [1.0] — État initial du projet
- Structure du projet (RAW / Bronze / Silver / Gold)
- Modules d'ingestion (`load_raw.py`, `to_bronze.py`)
- Modules de nettoyage Silver (`silver_cleaning.py`)
- Base de pipeline Gold (`pipeline.py`)
