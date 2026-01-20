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

## 🛠️ Stack Technique

### Infrastructure
- **MinIO**: Stockage objet (data lake)
- **PostgreSQL**: Base de données Prefect
- **Prefect**: Orchestration des workflows

### Python
- **Pandas**: Manipulation de données
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
│   ├── silver_transformation.py # Transformation Silver
│   ├── gold_aggregation.py    # Agrégation Gold
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
