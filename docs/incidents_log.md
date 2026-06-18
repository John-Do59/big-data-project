# Journal des incidents — Olist Pipeline

Ce document recense les principaux incidents rencontrés lors du développement de la pipeline Big Data Olist, leurs causes, les actions correctives mises en œuvre et les enseignements retenus.

---

# Incident n°1 — Fan-out des données lors des jointures Gold

**Date :** Jour 2 (mercredi)
**Découvert par :** Amaury
**Sévérité :** Haute

## Symptôme

Le chiffre d'affaires total calculé par la couche Gold était anormalement élevé, plusieurs fois supérieur à la valeur attendue.

## Cause identifiée

La couche Gold reposait initialement sur une table consolidée unique (« super table ») obtenue par jointure entre :

```sql
orders
JOIN order_items
```

Cette jointure a généré un phénomène de **fan-out** : chaque commande était dupliquée autant de fois qu'elle contenait d'articles, provoquant une multiplication artificielle des montants lors des agrégations.

## Solution appliquée

* Suppression de la table consolidée unique.
* Refactorisation de la couche Gold en modules KPI indépendants.
* Chaque KPI réalise uniquement les jointures nécessaires à son calcul.

## Leçon retenue

Toujours contrôler la volumétrie avant et après une jointure :

```python
before = df.count()
after = joined_df.count()
```

Une augmentation inattendue du nombre de lignes est souvent le premier indicateur d'un problème de fan-out.

---

# Incident n°2 — Appel de `run_quality_checks()` hors fonction

**Date :** Jour 3 (jeudi)
**Découvert par :** Amaury
**Sévérité :** Moyenne

## Symptôme

La pipeline échouait immédiatement au démarrage avec l'erreur :

```python
NameError: name 'silver' is not defined
```

## Cause identifiée

L'appel suivant était placé directement dans le module `main.py` :

```python
run_quality_checks(silver)
```

Le code étant exécuté lors de l'import du module, la variable `silver` n'était pas encore créée.

## Solution appliquée

Déplacement de l'appel dans la fonction `main()` après la construction du dictionnaire Silver :

```python
silver = build_silver_layer(...)
run_quality_checks(silver)
```

## Leçon retenue

Tout code placé au niveau d'un module Python est exécuté à l'import.

Les appels dépendant de variables locales doivent être exécutés à l'intérieur d'une fonction ou protégés par :

```python
if __name__ == "__main__":
    main()
```

---

# Incident n°3 — Types de colonnes incorrects après chargement CSV

**Date :** Jour 1 (mardi)
**Découvert par :** Alexandre
**Sévérité :** Moyenne

## Symptôme

Les colonnes :

* `price`
* `freight_value`

étaient chargées en type `string` au lieu de `double`, empêchant le calcul des indicateurs financiers.

## Cause identifiée

L'inférence automatique des types via :

```python
inferSchema=True
```

n'a pas correctement interprété certaines colonnes numériques à cause de valeurs manquantes ou de formats hétérogènes.

## Solution appliquée

Ajout d'un cast explicite dans les fonctions de nettoyage Silver :

```python
col("price").cast("double")
col("freight_value").cast("double")
```

et

```python
col("payment_value").cast("double")
```

## Leçon retenue

Ne jamais se reposer uniquement sur l'inférence automatique des types pour les colonnes critiques.

Les conversions doivent être explicitement définies dans la couche Silver.

---

# Incident n°4 — Valeurs nulles sur les dates de livraison

**Date :** Jour 2 (mercredi)
**Découvert par :** Fatima
**Sévérité :** Faible

## Symptôme

Le calcul du délai moyen de livraison retournait de nombreuses valeurs nulles et des résultats incohérents.

## Cause identifiée

Les commandes non livrées (`shipped`, `processing`, `canceled`, etc.) ne possèdent pas de valeur dans la colonne :

```text
order_delivered_customer_date
```

Ces lignes étaient néanmoins incluses dans les calculs logistiques.

## Solution appliquée

Ajout d'un filtrage préalable :

```python
.filter(
    col("order_delivered_customer_date").isNotNull()
)
```

avant le calcul :

* du délai moyen de livraison ;
* du taux de retard.

## Leçon retenue

Les indicateurs logistiques doivent être calculés uniquement sur les commandes effectivement livrées.

Toujours vérifier la présence des dates métier nécessaires avant toute opération de calcul.

---

# Synthèse

| Incident                   | Sévérité | Domaine                  |
| -------------------------- | -------- | ------------------------ |
| Fan-out des jointures Gold | Haute    | Modélisation des données |
| Appel hors fonction        | Moyenne  | Architecture Python      |
| Mauvais typage Spark       | Moyenne  | Qualité des données      |
| Dates de livraison nulles  | Faible   | Logique métier           |

## Enseignements généraux

* Vérifier systématiquement les cardinalités lors des jointures.
* Contrôler explicitement les types de données.
* Centraliser les contrôles qualité dans la couche Silver.
* Filtrer les données incomplètes avant les calculs métier.
* Développer des KPIs modulaires afin de limiter les effets de bord entre traitements.
