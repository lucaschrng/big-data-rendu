# 📊 Data Lake Analytics Project

Architecture Bronze-Silver-Gold avec pipeline ELT complet et dashboard Streamlit.

## 🏗️ Architecture

```
Sources → Bronze → Silver → Gold → Dashboard
         (Raw)   (Clean)  (Business)  (Viz)
```

### Couches de données

- **Bronze**: Données brutes, copie fidèle des sources
- **Silver**: Données nettoyées, validées et standardisées
- **Gold**: Agrégations métier, KPIs et métriques business

## 🚀 Quick Start

### 1. Installation

```bash
# Installer les dépendances
uv sync
```

### 2. Démarrer l'infrastructure

```bash
# Lancer MinIO et Prefect
docker-compose up -d

# Vérifier que les services sont actifs
docker-compose ps
```

**Services disponibles:**
- MinIO Console: http://localhost:9001 (minioadmin / minioadmin)
- Prefect UI: http://localhost:4200

### 3. Générer les données (optionnel)

```bash
# Générer de nouvelles données de test
python script/generate_data.py
```

### 4. Exécuter le pipeline ELT

```bash
# Pipeline complet (Bronze → Silver → Gold)
cd flows
python run_pipeline.py
```

### 5. Lancer le dashboard

```bash
# Démarrer Streamlit
streamlit run dashboard.py
```

Le dashboard sera accessible sur http://localhost:8501

## 📊 Dashboard Streamlit

Le dashboard contient 6 pages:

### 🏠 Overview
- KPIs principaux (revenus, commandes, clients)
- Segmentation clients (actifs/inactifs, high-value)
- Top performers (produit, pays, client)
- Tendances récentes (30 derniers jours)

### 📈 Temporal Analysis
- Analyse quotidienne, hebdomadaire ou mensuelle
- Graphiques de tendance des revenus
- Taux de croissance période à période
- Moyennes mobiles (7 jours)

### 👥 Client Analytics
- Analyse RFM (Recency, Frequency, Monetary)
- Distribution des clients par segment
- Top 10 clients par dépenses
- Lifetime value et fréquence d'achat

### 📦 Product Analytics
- Revenus par produit
- Parts de marché
- Analyse des prix (min, max, moyenne)
- Clients uniques par produit

### 🌍 Geographic Analysis
- Revenus par pays
- Parts de marché géographiques
- Panier moyen par pays
- Nombre de clients par pays

### 📊 Statistics
- Distributions statistiques complètes
- Box plots et percentiles
- Métriques globales
- Statistiques descriptives

## ⚡ Apache Spark (Big Data)

### Démarrer le cluster Spark

```bash
# Lancer le cluster complet (1 master + 2 workers)
docker-compose up -d spark-master spark-worker-1 spark-worker-2

# Vérifier le cluster
docker-compose ps
```

**Spark UI disponible sur:** http://localhost:8080

### Exécuter le pipeline Spark

```bash
cd flows

# Pipeline Spark uniquement
python spark_silver_transformation.py
python spark_gold_aggregation.py

# Ou utiliser le benchmark pour comparer Pandas vs Spark
python benchmark.py
```

### Benchmark Pandas vs Spark

```bash
cd flows

# Benchmark complet (Pandas + Spark)
python benchmark.py

# Options disponibles
python benchmark.py --pandas-only     # Seulement Pandas
python benchmark.py --spark-only      # Seulement Spark
python benchmark.py --spark-master spark://spark-master:7077  # Cluster distant

# Les résultats sont sauvegardés dans data/benchmark_results.json
```

#### Exemple de sortie benchmark

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TIMING COMPARISON (seconds)                          │
├───────────────────────┬───────────────────┬───────────────────┬─────────────┤
│ Step                  │ Pandas            │ Spark             │ Speedup     │
├───────────────────────┼───────────────────┼───────────────────┼─────────────┤
│ Bronze Ingestion      │            0.1234 │            0.0000 │         N/A │
│ Silver Transformation │            0.2345 │            1.5678 │       0.15x │
│ Gold Aggregation      │            0.3456 │            2.3456 │       0.15x │
├───────────────────────┼───────────────────┼───────────────────┼─────────────┤
│ TOTAL                 │            0.7035 │            3.9134 │       0.18x │
└───────────────────────┴───────────────────┴───────────────────┴─────────────┘

🏆 Winner: PANDAS
   Reason: Lower overhead for small datasets
```

> **💡 Note**: Spark a un overhead de démarrage. Avec des datasets plus volumineux (millions de lignes), Spark montrera des gains de performance significatifs grâce au traitement distribué.

## 🚀 Base NoSQL & Dashboarding (Nouveau)

Une couche opérationnelle temps-réel a été ajoutée :

1.  **MongoDB** : Base NoSQL pour les données Gold (Lecture rapide).
2.  **FastAPI** : API REST exposant les KPIs et données analytiques.
3.  **Streamlit** : Dashboard interactif consommant l'API.
4.  **Metabase** : Outil BI open-source pour l'exploration de données.

### 🔄 Pipeline de Rafraîchissement

Pour générer les données Gold (Parquet) et les charger dans MongoDB :

```bash
uv run flows/benchmark_refresh.py
```
*Temps de refresh moyen : ~6 secondes*

### 🌐 Lancer l'API et le Dashboard

1.  **Démarrer l'API** (Port 8000) :
    ```bash
    uv run uvicorn api.main:app --reload --port 8000
    ```

2.  **Démarrer le Dashboard Unifié** (Port 8501) :
    ```bash
    uv run streamlit run dashboard.py
    ```

    > **Nouveau** : Le dashboard possède maintenant un sélecteur de source de données dans la barre latérale :
    > - **Data Lake (Historical/MinIO)** : Visualisation des fichiers statiques (CSV/Parquet) du bucket Gold.
    > - **Operational (Live/MongoDB)** : Visualisation temps-réel via l'API FastAPI et MongoDB.

### 📊 Accès à Metabase

Metabase est disponible sur [http://localhost:3000](http://localhost:3000).
- **Setup** : Suivez l'assistant d'installation.
- **Connexion BDD** :
    - Type : PostgreSQL
    - Host : `postgres`
    - Port : `5432`
    - Database : `prefect` (ou autre si configuré)
    - User/Pass : `prefect` / `prefect`

## 📊 Résultats du Benchmark (Optimisé)

Sur un MacBook Pro (M1/M2/M3) avec le dataset par défaut (2M clients, 10M achats) :

| Étape | Pandas (Local) | Spark (Local Optimisé) |
|-------|---------------:|------------------------|
| Ingestion Bronze | ~14s | N/A (Partagé) |
| Transformation Silver | ~42s | ~76s |
| Agrégation Gold | ~91s | ~176s |
| **Total** | **~147s** | **~252s** |

### 💡 Analyse des Performances

1.  **Pourquoi Pandas est plus rapide ici ?**
    *   Le dataset (12M lignes) tient entièrement en RAM.
    *   Pandas n'a pas l'overhead de démarrage de JVM/Spark (1-2s par job).
    *   Les opérations se font "in-memory" sans sérialisation/désérialisation complexe.

2.  **Quand utiliser Spark ?**
    *   Si le dataset dépasse la RAM (ex: > 100M lignes ou > 50GB).
    *   Si les calculs nécessitent un cluster distribué (plusieurs machines).
    *   Pour des jointures complexes sur des données massives.

3.  **Optimisations Spark appliquées :**
    *   **Broadcast Joins** pour les tables de dimension (Clients).
    *   **Partitioning** intelligent (8 partitions en local).
    *   Suppression des actions `.count()` inutiles (Lazy Evaluation).
    *   **Coalesce(1)** pour les agrégations globales (petits résultats).

## 🛠️ Stack Technique

### Infrastructure
- **MinIO**: Stockage objet (data lake)
- **PostgreSQL**: Base de données Prefect
- **Prefect**: Orchestration des workflows
- **Apache Spark**: Traitement distribué Big Data (1 master + 2 workers)

### Python
- **Pandas**: Manipulation de données (single-node)
- **PySpark**: Manipulation de données distribuée
- **Prefect**: Orchestration
- **Streamlit**: Dashboard interactif
- **Plotly**: Visualisations

## 📁 Structure du projet

```
.
├── data/                       # Données sources (CSV)
│   ├── clients.csv
│   └── purchases.csv
├── flows/                      # Flows Prefect
│   ├── config.py              # Configuration MinIO/Prefect
│   ├── bronze_ingestion.py    # Ingestion Bronze
│   ├── silver_transformation.py # Transformation Silver (Pandas)
│   ├── gold_aggregation.py    # Agrégation Gold (Pandas)
│   ├── spark_silver_transformation.py # Transformation Silver (Spark)
│   ├── spark_gold_aggregation.py # Agrégation Gold (Spark)
│   ├── benchmark.py           # Benchmark Pandas vs Spark
│   └── run_pipeline.py        # Pipeline complet
├── script/
│   └── generate_data.py       # Génération de données
├── dashboard.py               # Dashboard Streamlit
├── docker-compose.yml         # Infrastructure
├── pyproject.toml            # Dépendances
└── README.md
```

## 📦 Buckets MinIO

### sources/
- Fichiers sources temporaires
- Point d'entrée des données

### bronze/
- Données brutes archivées
- Source of truth immuable

### silver/
- Données nettoyées et validées
- `clients.csv`: Clients validés
- `purchases.csv`: Achats validés
- `quality_report.json`: Rapport de qualité

### gold/
- Données agrégées et KPIs
- `fact_sales.csv`: Table de faits
- `client_kpis.csv`: KPIs clients
- `product_analytics.csv`: Analytics produits
- `country_analytics.csv`: Analytics pays
- `daily_sales.csv`: Agrégations quotidiennes
- `weekly_sales.csv`: Agrégations hebdomadaires
- `monthly_sales.csv`: Agrégations mensuelles
- `statistical_distributions.json`: Statistiques
- `gold_summary.json`: Résumé exécutif

## 🔄 Workflows

### Bronze Ingestion
```bash
cd flows
python bronze_ingestion.py
```
- Upload des CSV vers MinIO sources
- Copie vers la couche Bronze

### Silver Transformation
```bash
cd flows
python silver_transformation.py
```
- Nettoyage des valeurs nulles
- Standardisation des dates
- Validation des données
- Déduplication
- Génération du rapport de qualité

### Gold Aggregation
```bash
cd flows
python gold_aggregation.py
```
- Création de la fact table
- Calcul des KPIs clients
- Analytics produits et pays
- Agrégations temporelles
- Statistiques descriptives

## 📊 KPIs disponibles

### Clients
- Total spent, average order value
- Purchase frequency
- Customer lifetime value
- Recency (days since last purchase)
- Segmentation RFM

### Produits
- Total revenue, quantity sold
- Average price, price range
- Unique customers
- Top countries per product

### Géographie
- Revenue by country
- Market share
- Average order value by country
- Customers per country

### Temporel
- Daily/weekly/monthly revenue
- Growth rates (%)
- Moving averages
- Trends and seasonality

## 🎯 Cas d'usage

### Marketing
- Identifier les clients à risque (high-value + inactifs)
- Segmentation pour campagnes ciblées
- Analyse de la rétention

### Finance
- Reporting mensuel automatique
- Prévisions basées sur tendances
- Analyse de croissance

### Produit
- Optimisation du catalogue
- Stratégie pricing
- Analyse cross-sell

### Opérations
- Monitoring quotidien
- Détection d'anomalies
- Planification de capacité

## 🔧 Configuration

### Variables d'environnement (.env)

```bash
# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=False

# Prefect
PREFECT_API_URL=http://localhost:4200/api

# Database
SQLITE_DB_PATH=./data/database/analytics.db
```

## 📚 Documentation

- `GOLD_LAYER.md`: Documentation détaillée de la couche Gold
- Voir les docstrings dans chaque fichier Python

## 🐛 Troubleshooting

### MinIO ne démarre pas
```bash
docker-compose down
docker-compose up -d minio
```

### Prefect ne se connecte pas
```bash
# Vérifier l'URL
echo $PREFECT_API_URL
# Devrait être: http://localhost:4200/api
```

### Dashboard ne charge pas les données
```bash
# Vérifier que le pipeline a été exécuté
cd flows
python run_pipeline.py

# Vérifier les buckets MinIO
# Aller sur http://localhost:9001
```

### Erreur de dépendances
```bash
# Réinstaller
uv sync --reinstall
```

## 🎓 Prochaines étapes

1. **Ajouter des métriques avancées**
   - CLV prédictif
   - Probabilité de churn
   - Cohort analysis

2. **Automatiser les rapports**
   - Scheduling quotidien/hebdomadaire
   - Envoi d'emails avec KPIs
   - Alertes sur anomalies

3. **Connecter un outil BI**
   - Metabase, Superset, ou Tableau
   - Dashboards partagés
   - Rapports automatiques

4. **Optimisations**
   - Utiliser Parquet au lieu de CSV
   - Partitionnement par date
   - Traitement incrémental

## 📝 License

MIT

## 👥 Auteur

Projet de cours - Data Lake Architecture
