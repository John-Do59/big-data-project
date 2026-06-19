# Documentation qualité des données — Dataset Olist

## 1. Méthodologie

Les contrôles qualité sont exécutés automatiquement sur la couche silver via `src/transformations/quality_checks.py`, après nettoyage de chaque table. Cinq types de contrôles sont appliqués :

- Détection des valeurs nulles, colonne par colonne
- Détection des doublons sur la clé primaire de chaque table
- Détection des montants négatifs (price, freight_value, payment_value)
- Contrôle de cohérence des dates (livraison postérieure à l'achat)
- Validation des scores d'avis (doivent être compris entre 1 et 5)

## 2. Problèmes détectés et traitements appliqués

| Table | Problème détecté | Traitement appliqué |
|-------|-------------------|----------------------|
| order_reviews | Colonnes décalées : des retours à la ligne (`\n`) dans les commentaires clients faisaient basculer des dates dans la colonne `review_score` | Rechargement du CSV avec l'option `multiLine=True` |
| geolocation | ~1 000 163 lignes pour quelques dizaines de milliers de codes postaux distincts (relevés GPS multiples par zone) | Agrégation par `zip_code_prefix` avec moyenne des coordonnées lat/lng |
| orders | Quelques commandes affichaient une date de livraison antérieure à la date d'achat | Lignes filtrées, valeurs nulles conservées (commandes non livrées) |
| Toutes les tables | Doublons sur les clés primaires (order_id, customer_id, product_id...) | `dropDuplicates()` sur la clé métier de chaque table |

## 3. Gestion des valeurs nulles

| Table | Colonne | Stratégie | Justification |
|-------|---------|-----------|----------------|
| orders | order_status | Remplacement par `"unknown"` | Préserve la ligne plutôt que de la supprimer |
| orders | dates de livraison | Conservées en null | Commande non encore livrée, donnée légitimement absente |
| customers | customer_city / customer_state | Remplacement par `"unknown"` | Évite de perdre la commande associée |
| order_items | price / freight_value | Remplacement par `0.0` | Permet l'agrégation sans fausser les totaux existants |
| payments | payment_type | Remplacement par `"unknown"` | Conserve la transaction pour les volumes globaux |
| payments | payment_value | Remplacement par `0.0` | Évite une erreur de cast lors des sommes |
| reviews | review_score | Remplacement par `3` (valeur neutre) | Évite de biaiser la moyenne vers le haut ou le bas |
| reviews | review_comment_title / message | Remplacement par chaîne vide | Commentaire facultatif, non bloquant |
| products | product_category_name | Remplacement par `"unknown"` | Catégorie non renseignée à la source |
| products | dimensions (poids, longueur...) | Remplacement par `0` | Donnée manquante, ne doit pas bloquer les jointures |

## 4. Contrôles de cohérence métier

**Dates de commande**
Une commande ne peut pas être livrée avant d'avoir été achetée, ni approuvée avant d'avoir été passée. Les lignes violant ces règles sont écartées (valeurs nulles tolérées pour les commandes en cours) :

```python
.filter(
    (col("order_delivered_customer_date") >= col("order_purchase_timestamp")) |
    col("order_delivered_customer_date").isNull()
)
```

**Scores d'avis**
Les scores doivent être compris entre 1 et 5. Le contrôle `check_review_scores` détecte toute valeur hors de cette plage après nettoyage.

**Montants**
Les colonnes `price`, `freight_value` et `payment_value` sont contrôlées pour détecter d'éventuelles valeurs négatives, incohérentes pour une transaction commerciale.

## 5. Volumétrie par table (raw → silver)

| Table | Lignes raw | Lignes silver | Évolution |
|-------|-----------|----------------|-----------|
| orders | 99 441 | ~99 416 | Suppression des incohérences de dates |
| customers | 99 441 | 99 441 | Stable |
| order_items | 112 650 | 112 650 | Stable |
| order_payments | 103 886 | 103 886 | Stable |
| order_reviews | 100 000 | ~99 224 | Filtrage des scores invalides après correction du parsing CSV |
| products | 32 951 | 32 951 | Stable |
| sellers | 3 095 | 3 095 | Stable |
| product_category_name_translation | 71 | 71 | Stable |
| geolocation | 1 000 163 | ~19 000 | Agrégation par code postal (moyenne des coordonnées) |

## 6. Limites connues

- Les valeurs nulles remplacées par des valeurs par défaut (`"unknown"`, `0.0`, score neutre `3`) peuvent légèrement diluer certains indicateurs si elles sont nombreuses sur une colonne donnée ; leur volume n'a pas été mesuré précisément en proportion du total.
- Les incohérences de dates corrigées concernent un nombre marginal de lignes (anomalies du dataset source), sans impact significatif sur les KPI globaux.
- Aucune analyse de la qualité des champs texte libres (commentaires d'avis) n'a été réalisée (pas de détection de spam, doublons de contenu, etc.).