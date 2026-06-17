D'après tes schémas, voici ce qu'il faut faire demain :

**Typage**
- `price`, `freight_value`, `payment_value` → float
- `order_item_id`, `payment_sequential`, `payment_installments`, `review_score` → integer
- `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`, `product_photos_qty` → integer
- `geolocation_lat`, `geolocation_lng` → float
- Toutes les colonnes `*_date` et `*_timestamp` → timestamp

**Valeurs nulles**
- `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date` → commandes non livrées, à garder mais documenter
- `review_comment_title`, `review_comment_message` → normal, pas obligatoire de laisser un commentaire
- `product_category_name` → joindre avec la table category pour traduire, nulls à signaler

**Doublons**
- `geolocation` → beaucoup de doublons sur le zip_code, dédoublonner
- `order_reviews` → vérifier les `review_id` en double

**Incohérences**
- `order_delivered_customer_date` < `order_purchase_timestamp` → livraison avant achat, impossible
- `order_estimated_delivery_date` vs `order_delivered_customer_date` → calculer les retards