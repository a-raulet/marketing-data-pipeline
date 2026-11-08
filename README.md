# Marketing Data Pipeline

**Business problem.** Marketing teams spread budget across paid search, paid social, LinkedIn and email, but spend, sessions and revenue live in separate exports. Without one daily view of ROAS, cost per conversion and conversion rate per channel, budget decisions rely on gut feeling.

**Key result.** An end-to-end, reproducible pipeline: simulated daily data for 5 channels (Sep 1 – Nov 23, 2024, fixed seed) loaded into BigQuery, modeled with dbt into a staging view with ROAS, cost per conversion, conversion rate and weekly/monthly grains, and served in a filterable Streamlit dashboard. The data is synthetic, so the project demonstrates the pipeline, not market findings.


**Stack.** Python 3.13, Google BigQuery, dbt-core + dbt-bigquery, Streamlit, Plotly, pandas.

**How to run.**
```bash
poetry install                        # or: pip install -r requirements.txt
# create .env with GCP_PROJECT_ID and GCP_CREDENTIALS_PATH (see Installation below)
poetry run python src/extract_data.py # writes data/raw/marketing_data.csv
poetry run python src/load_bigquery.py
cd dbt_marketing && poetry run dbt run && cd ..
poetry run streamlit run src/dashboard.py
```

Live demo: https://marketing-data-pipeline-demo.streamlit.app

---

## Description

Ce projet implémente un pipeline ETL complet permettant de :
- **Extraire** des données marketing simulées réalistes
- **Charger** les données dans Google BigQuery
- **Transformer** les données brutes en métriques analytiques via dbt
- **Visualiser** les KPIs dans un dashboard interactif Streamlit

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Extract   │───▶│    Load     │───▶│  Transform  │───▶│  Visualize  │
│   (Python)  │    │  (BigQuery) │    │    (dbt)    │    │ (Streamlit) │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │                  │
   CSV data         Raw tables          Views SQL         Dashboard
```

## Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Langage | Python 3.13+ |
| Base de données | Google BigQuery |
| Transformation | dbt-core, dbt-bigquery |
| Visualisation | Streamlit, Plotly |
| Data processing | Pandas, NumPy |
| Déploiement | Streamlit Cloud |

## Prérequis

- Python 3.13 ou supérieur
- Un compte Google Cloud Platform avec BigQuery activé
- Un fichier de credentials GCP (service account JSON)

## Installation

### 1. Cloner le repository

```bash
git clone https://github.com/a-raulet/marketing-data-pipeline.git
cd marketing-data-pipeline
```

### 2. Installer les dépendances

Avec Poetry (recommandé) :
```bash
poetry install
```

Ou avec pip :
```bash
pip install -r requirements.txt
```

### 3. Configurer les variables d'environnement

Créer un fichier `.env` à la racine du projet :

```env
GCP_PROJECT_ID=votre-project-id
GCP_CREDENTIALS_PATH=./credentials.json
DATASET_RAW=marketing_raw
DATASET_ANALYTICS=marketing_analytics
```

### 4. Configurer dbt

Créer le fichier `~/.dbt/profiles.yml` :

```yaml
dbt_marketing:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: votre-project-id
      dataset: marketing_analytics
      keyfile: /chemin/vers/credentials.json
      threads: 4
      timeout_seconds: 300
      location: US
```

## Utilisation

### Étape 1 : Extraction des données

Génère des données marketing simulées pour plusieurs canaux :

```bash
python src/extract_data.py
```

Les données sont sauvegardées dans `data/raw/marketing_data.csv`.

**Canaux simulés :**
- Google Ads
- Facebook Ads
- LinkedIn
- Email
- Direct (trafic organique)

### Étape 2 : Chargement dans BigQuery

Charge les données CSV dans BigQuery :

```bash
python src/load_bigquery.py
```

Les données sont chargées dans la table `marketing_raw.daily_performance`.

### Étape 3 : Transformation avec dbt

Exécute les transformations dbt :

```bash
cd dbt_marketing
dbt run
```

Cela crée la vue `marketing_analytics.stg_marketing_daily` avec les métriques calculées :
- **ROAS** (Return on Ad Spend)
- **Taux de conversion**
- **Coût par conversion**
- **Revenu par session**

### Étape 4 : Lancement du dashboard

```bash
streamlit run src/dashboard.py
```

Le dashboard est accessible sur `http://localhost:8501`.

## Structure du Projet

```
marketing-data-pipeline/
├── src/
│   ├── extract_data.py      # Génération des données simulées
│   ├── load_bigquery.py     # Chargement vers BigQuery
│   ├── dashboard.py         # Dashboard Streamlit
│   └── test_setup.py        # Tests de configuration
│
├── dbt_marketing/
│   ├── dbt_project.yml      # Configuration dbt
│   └── models/
│       └── staging/
│           ├── sources.yml              # Définition des sources
│           └── stg_marketing_daily.sql  # Modèle de staging
│
├── data/
│   └── raw/                 # Données CSV brutes
│
├── sql_examples/            # Requêtes SQL d'exemple
├── .streamlit/              # Configuration Streamlit
├── pyproject.toml           # Dépendances Poetry
├── requirements.txt         # Dépendances pip
└── .env                     # Variables d'environnement
```

## Dashboard

Le dashboard interactif inclut :

### KPIs Principaux
- Dépenses totales (spend)
- Revenu total avec ROI
- Nombre de conversions
- ROAS global

### Visualisations
- **Tendance journalière** : Évolution des dépenses vs revenus
- **Performance par canal** : Comparaison des sources marketing
- **ROAS par source** : Rentabilité de chaque canal
- **Funnel de conversion** : Sessions → Conversions

### Filtres
- Sélection de la plage de dates
- Filtrage par canal marketing

## Schéma des Données

### Table brute : `marketing_raw.daily_performance`

| Colonne | Type | Description |
|---------|------|-------------|
| date | DATE | Date de la performance |
| source | STRING | Canal marketing |
| sessions | INT | Nombre de sessions |
| conversions | INT | Nombre de conversions |
| revenue | FLOAT | Revenu généré (USD) |
| spend | FLOAT | Dépenses marketing (USD) |

### Vue transformée : `marketing_analytics.stg_marketing_daily`

| Colonne | Type | Description |
|---------|------|-------------|
| date | DATE | Date normalisée |
| week_start_date | DATE | Début de semaine |
| month_start_date | DATE | Début de mois |
| marketing_source | STRING | Canal marketing |
| sessions | INT | Sessions |
| conversions | INT | Conversions |
| revenue | FLOAT | Revenu |
| spend | FLOAT | Dépenses |
| roas | FLOAT | Return on Ad Spend |
| cost_per_conversion | FLOAT | Coût par conversion |
| conversion_rate | FLOAT | Taux de conversion |
| is_organic_channel | BOOL | Canal organique (spend=0) |
| is_weekend | BOOL | Indicateur week-end |

## Déploiement sur Streamlit Cloud

### 1. Pousser le code sur GitHub

```bash
git add .
git commit -m "Deploy to Streamlit Cloud"
git push origin main
```

### 2. Configurer Streamlit Cloud

1. Connecter le repository GitHub sur [share.streamlit.io](https://share.streamlit.io)
2. Définir le fichier principal : `src/dashboard.py`
3. Ajouter les secrets dans les paramètres :

```toml
GCP_PROJECT_ID = "votre-project-id"

[gcp_service_account]
type = "service_account"
project_id = "votre-project-id"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
```

## Tests

Vérifier la configuration et les connexions :

```bash
python src/test_setup.py
```

Ce script valide :
- L'installation des dépendances
- La présence du fichier credentials
- La connexion à BigQuery
- La disponibilité des données

## Licence

MIT License

## Auteur

Projet développé dans le cadre d'un pipeline de données marketing analytiques.
