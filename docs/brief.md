#  Brief Projet — Pipeline Big Data Olist (PySpark)

##  Contexte

Vous êtes développeur·se Data/IA au sein d’une équipe chargée d’aider une entreprise e-commerce à mieux exploiter ses données.

L’entreprise dispose de plusieurs fichiers CSV issus de son activité :

- commandes  
- clients  
- articles commandés  
- paiements  
- produits  
- vendeurs  
- avis clients  
- géolocalisation  

Ces données sont riches, mais dispersées dans plusieurs fichiers et ne peuvent pas être exploitées directement par les équipes métier.

Aujourd’hui, les analyses sont réalisées manuellement et ne sont pas reproductibles. L’entreprise souhaite mettre en place une pipeline Big Data locale capable de transformer ces données brutes en données nettoyées, structurées et analysables.

---

##  Dataset

Vous travaillerez avec le dataset public **Brazilian E-Commerce Public Dataset by Olist** (Kaggle).

Ce dataset permet d’analyser plusieurs aspects métier :

- chiffre d’affaires  
- catégories de produits  
- moyens de paiement  
- performance des vendeurs  
- délais de livraison  
- satisfaction client  
- répartition géographique des ventes  

---

##  Mission

Construire une pipeline avec **PySpark** permettant de transformer des fichiers CSV bruts en tables analytiques exploitables.

---

##  Architecture Data Lake

Vous devrez organiser les données selon une logique simple :

- **raw** : fichiers CSV d’origine, non modifiés  
- **bronze** : données chargées avec Spark et sauvegardées en format exploitable  
- **silver** : données nettoyées, typées et fiabilisées  
- **gold** : données analytiques prêtes pour les KPIs métier  

---

##  Pipeline attendue

La pipeline doit permettre :

- chargement de plusieurs fichiers CSV  
- correction des types de données  
- gestion des valeurs nulles  
- identification des incohérences  
- jointures entre tables principales  
- agrégations métier  
- export en format Parquet  

---

##  Contraintes techniques

- PySpark obligatoire pour la pipeline principale  
- Pandas uniquement pour vérification ou visualisation ponctuelle  
- pas de pipeline principale en Pandas  

---

##  KPIs attendus

Au minimum :

- chiffre d’affaires total  
- chiffre d’affaires mensuel  
- panier moyen  
- top catégories produits  
- top vendeurs  
- délai moyen de livraison  
- taux de retard  
- note moyenne client  
- impact des retards sur la satisfaction  
- répartition des paiements  

---

##  Organisation du projet

### Mardi — Découverte & ingestion
- exploration dataset  
- création repo Git  
- ingestion Spark  
- analyse schémas  
- identification clés de jointure  

### Mercredi — Silver layer
- nettoyage données  
- typage  
- gestion nulls  
- suppression doublons  
- export Parquet  

### Jeudi — Gold layer
- jointures  
- enrichissement données  
- calcul KPIs  
- export tables analytiques  

### Vendredi — Finalisation
- pipeline complète  
- README  
- architecture  
- data quality report  
- préparation soutenance  

---

##  Restitution (20 min)

La présentation doit inclure :

- contexte métier  
- structure dataset  
- architecture raw / bronze / silver / gold  
- traitements réalisés  
- problèmes de qualité  
- indicateurs produits  
- démonstration pipeline  
- incident rencontré  
- limites du projet  

---

##  Évaluation collective

- pipeline PySpark fonctionnelle  
- organisation projet  
- qualité du nettoyage  
- cohérence des couches  
- pertinence des jointures  
- KPIs métier  
- export Parquet  
- qualité README  
- schéma architecture  
- qualité restitution orale  

---

##  Vérification individuelle

Chaque membre doit expliquer :

- rôle de Spark  
- différence Pandas vs Spark  
- DataFrame Spark  
- logique data lake  
- format Parquet  
- tables Olist  
- jointures  
- nettoyage  
- KPIs  
- contribution personnelle  
- incident rencontré  

---

##  Livrables

1. dépôt Git propre  
2. pipeline PySpark fonctionnelle  
3. structure data lake  
4. README  
5. documentation qualité  
6. présentation finale  

---

##  Critères de performance

- pipeline exécutable  
- lecture CSV Spark  
- schémas analysés  
- types corrigés  
- gestion dates / montants / IDs  
- traitement des nulls justifié  
- jointures cohérentes  
- séparation raw/bronze/silver/gold respectée  
- fichiers bruts non modifiés  
- export Parquet OK  
- au moins 8 KPIs  
- interprétation métier claire  
- README reproductible  
- schéma architecture fidèle  
- journal incidents présent  
- pas de dépendance Pandas pour pipeline  
- présentation claire et structurée (20 min)  