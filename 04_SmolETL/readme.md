## 4. Smol ETL
Objectifs : construire un POC d'ETL avec DuckDB
Use case métier : disposer d'une solution rapide d'analytics en local

Structure : 1 base duckdb

Steps : 
### 1. BRONZE : staging zone
- Objectif : Extraction et chargement des fichiers sources sans transformation
- Tables : 
    - trips_bronze
    - zones_bronze

### 2. SILVER : transformation layer
- Objectif : Nettoyage et transformation des données
- Tables : 
    - trips_silver
    
- Opérations :
    - Gestion des valeurs manquantes
    - Standardisation des formats (all timestamps to UTC)
    - Nettoyage des valeurs nulles (zero fare, zero distance, or invalid passenger counts)
    - Enrichissement:
        - Joining Borough and Zone_Name
        - Calcul Trip_Duration_Minutes

### 3. GOLD : business metrics
- Objectifs : Mise à disposition de données agrégées et structurées dans un modèle dimensionnel 
- Tables : 
    - fact_trips_day
    - dim_time
    - dim_zone
    - dim_rate
