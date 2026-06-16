# Architecture Data Pipeline

##  Modèle global

RAW → BRONZE → SILVER → GOLD

##  Description des couches

### RAW
Données CSV brutes du dataset Olist.

### BRONZE
Données ingérées avec PySpark (format Parquet).

### SILVER
Données nettoyées, typées et validées.

### GOLD
Tables analytiques pour KPIs métier.

##  Sources de données
- orders
- customers
- order_items
- payments
- products
- sellers
- reviews
- geolocation