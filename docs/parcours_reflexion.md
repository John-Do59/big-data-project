# Parcours de réflexion — Pipeline Big Data Olist

Ce document retrace la démarche suivie du mardi au jeudi, les blocages rencontrés et les choix faits pour y répondre.

---

## Mardi — Découverte du dataset et ingestion

**Mise en place de l'environnement**
Premier blocage avant même de toucher au dataset : `SparkSession.builder.getOrCreate()` échouait avec `JAVA_GATEWAY_EXITED`. PySpark s'appuie sur une JVM même pour du code 100% Python, et Java n'était pas installé sur la machine. Installation du JDK, résolu.

**Découverte des 9 tables**
Plutôt que d'ouvrir chaque CSV à l'œil, on a chargé les 9 fichiers avec Spark et écrit une boucle pour afficher schéma, nombre de lignes et colonnes de chaque table d'un coup. Pour identifier les clés de jointure entre les tables sans le faire à la main table par table, on a construit un dictionnaire `colonne → liste de tables` : on parcourt les colonnes de chaque DataFrame et on regroupe celles qui portent le même nom. Résultat : `order_id` relie orders/order_items/order_payments/order_reviews, `customer_id` relie orders/customers, `product_id` relie order_items/products, `seller_id` relie order_items/sellers, et `zip_code_prefix` relie customers/sellers/geolocation (sous des noms de colonnes légèrement différents selon la table).

**Premier export bronze**
Chargement de chaque CSV en DataFrame, export direct en Parquet sans transformation. Première observation : Spark écrit plusieurs fichiers `.parquet` dans un dossier plutôt qu'un seul fichier — comportement par partitions, volontairement conservé tel quel (pas de `.coalesce(1)`) car c'est la pratique standard en Big Data, même si ça surprend au premier abord.

---

## Mercredi — Nettoyage et zone silver

**Premier passage de typage**
Toutes les colonnes sortent du CSV en `string`. Plan établi table par table : caster les montants en `float`/`double`, les identifiants numériques en `integer`, les dates en `timestamp` via `to_timestamp()`.

**Incident majeur : `order_reviews` fait planter le cast**
En castant `review_score` en `integer`, Spark renvoyait `CAST_INVALID_INPUT` avec des valeurs comme `'2017-07-01 23:08:37'` ou `' os BOTOES '` au lieu de chiffres. Diagnostic en plusieurs étapes :
1. Vérification de toutes les valeurs distinctes de `review_score` avec `.distinct().show()` → confirmation que des dates et du texte s'étaient glissés dans la colonne.
2. Hypothèse initiale (fausse) : confusion avec `inferSchema=True`. En fait le chargement initial était bien en string par défaut — le vrai problème était ailleurs.
3. Cause réelle identifiée : certains commentaires clients (`review_comment_message`) contiennent des retours à la ligne. Sans le bon paramètre, le parseur CSV de Spark interprète ce `\n` comme une fin de ligne du fichier, ce qui décale toutes les colonnes suivantes — une date se retrouve alors dans la colonne `review_score`.
4. Résolution : rechargement du CSV avec `multiLine=True`, qui dit à Spark qu'un champ peut légitimement contenir un saut de ligne. Vérifié après coup : `review_score` ne contient plus que les valeurs 1 à 5.

Avant la découverte de `multiLine=True`, une solution de contournement avait été utilisée (`when(col().rlike("^[1-5]$"), ...).otherwise(None)`) pour filtrer les valeurs corrompues sans planter. Cette approche reste valable comme garde-fou mais le vrai fix est en amont, au chargement.

**Doublons et valeurs nulles**
- `dropDuplicates()` appliqué sur la clé primaire de chaque table (pas sur des colonnes secondaires comme `review_score`, qui n'a pas de sens comme critère de dédoublonnage).
- `geolocation` : ~1 000 163 lignes pour quelques dizaines de milliers de codes postaux, à cause de relevés GPS multiples par zone. Plutôt que de supprimer arbitrairement les doublons (quelle ligne garder ?), choix de **moyenne les coordonnées** par `zip_code_prefix` via `groupBy().agg(avg(...))`. La table passe à ~19 000 lignes sans perte d'information géographique.

**Contrôle d'incohérences**
Vérification que `order_delivered_customer_date >= order_purchase_timestamp` (une commande ne peut pas être livrée avant d'être achetée). Quelques dizaines de lignes incohérentes détectées et supprimées, en conservant les valeurs nulles (commandes pas encore livrées).

---

## Jeudi — Jointures, gold et KPI

**Premier essai de table gold unique**
Tentative initiale : une seule grande jointure `orders + customers + order_items + products + sellers + category + order_payments + order_reviews`. Résultat : 118 249 lignes, alors que `order_items` seul en comptait 112 650 — anomalie.

**Diagnostic du gonflement de lignes**
Vérification avec `groupBy("order_id", "order_item_id").count().filter("count > 1")` : certaines commandes apparaissaient en double. Investigation sur un cas précis (`filter(col("order_id").like(...))`) : une commande payée par carte de crédit **et** par voucher génère deux lignes dans `order_payments`. En joignant cette table sur `order_id`, chaque article de la commande était dupliqué autant de fois qu'il y avait de paiements. Idem potentiellement pour les avis multiples par commande.

**Décision d'architecture : plusieurs tables gold thématiques plutôt qu'une seule**
Plutôt que de forcer une fusion qui génère des doublons, choix de séparer la zone gold par thème métier, chacune ne joignant que les tables strictement nécessaires :
- `gold_sales` — ventes, catégories, vendeurs
- `gold_delivery_satisfaction` — livraison et avis clients
- `gold_payments` — paiements (conserve naturellement plusieurs lignes par commande)
- `gold_geo` — répartition géographique

**Anticipation des besoins futurs**
Question posée en cours de route : faut-il des tables minimalistes (une colonne par KPI strictement nécessaire) ou des tables plus larges anticipant des besoins futurs de l'équipe métier ? Relecture du brief, qui précise que la zone gold doit être *« prête à être utilisée pour produire des indicateurs métier »* — donc orientée vers la réutilisabilité plutôt que vers un seul calcul figé. Décision : élargir chaque table gold avec quelques colonnes contextuelles supplémentaires (ex. `seller_city` en plus de `seller_state` dans `gold_geo`) sans pour autant tout fusionner.

**Erreurs de jointure rencontrées et résolues**
- `AMBIGUOUS_REFERENCE` sur `customer_state` : la colonne existait des deux côtés d'une jointure répétée. Résolu avec des alias explicites (`.alias("cust")`, `.alias("geo")`) et des références qualifiées (`col("cust.customer_state")`).
- Erreur similaire sur `customer_zip_code_prefix` lors de la jointure géo, même cause, même résolution.

**Calcul des KPI**
15 indicateurs calculés au total (au-delà des 8 minimum demandés), répartis selon la table gold concernée :
- `gold_sales` → CA total, CA mensuel, panier moyen, top catégories, top vendeurs, CA par état, commandes par statut, CA par vendeur et état
- `gold_delivery_satisfaction` → délai moyen de livraison, taux de retard, note moyenne, impact des retards sur la satisfaction, note moyenne par catégorie, délai moyen par état
- `gold_payments` → répartition des moyens de paiement

---

## Convergence avec le travail des autres membres de l'équipe

En parallèle, un membre de l'équipe a transposé la même logique dans une structure de scripts `.py` modulaires (`src/ingestion`, `src/transformations`, `src/gold`) orchestrés par un `pipeline.py` unique. Comparaison avec le travail fait en notebook :

- Les jointures et les 15 KPI sont identiques, code pour code.
- Le fix `multiLine=True` a été directement intégré dans `load_raw.py`, en conditionnant l'option au nom du fichier `olist_order_reviews_dataset.csv` — alors qu'en notebook il a fallu le découvrir par diagnostic en cours de route.
- Pour `review_score`, la version script utilise `try_cast` (disponible dans sa version de PySpark), alternative plus directe au `when().rlike()` utilisé en notebook.
- Choix différent sur la gestion des valeurs nulles : la version script remplace systématiquement par des valeurs par défaut (`fillna`), alors que le travail en notebook supprimait certaines lignes incohérentes. Les deux approches sont défendables et ont été documentées comme telles plutôt que tranchées arbitrairement.
- Les contrôles qualité ont été formalisés en fonctions réutilisables (`check_nulls`, `check_duplicates`, `check_negative_values`, `check_date_coherence`, `check_review_scores`) générant un rapport, alors qu'en notebook les contrôles étaient faits ponctuellement à la demande.

Le README, le schéma d'architecture et la documentation qualité ont ensuite été harmonisés avec cette version `.py`, qui reflète plus fidèlement l'organisation finale du dépôt Git.