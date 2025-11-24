"""
Load data to BigQuery
Charge les données CSV dans BigQuery
"""

import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Charger variables d'environnement
load_dotenv()

def load_to_bigquery(
    csv_path,
    project_id,
    dataset_id,
    table_id,
    credentials_path
):
    """
    Charge un CSV dans BigQuery
    
    Args:
        csv_path: Chemin vers le fichier CSV
        project_id: ID du projet GCP
        dataset_id: Nom du dataset BigQuery
        table_id: Nom de la table BigQuery
        credentials_path: Chemin vers credentials.json
    """
    
    print(f"\n{'='*60}")
    print(f"📤 CHARGEMENT VERS BIGQUERY")
    print(f"{'='*60}\n")
    
    # Configurer credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    # Initialiser client BigQuery
    client = bigquery.Client(project=project_id)
    
    print(f"🔗 Connecté au projet : {project_id}")
    
    # Créer dataset s'il n'existe pas
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        print(f"📁 Dataset '{dataset_id}' existe déjà")
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✨ Dataset '{dataset_id}' créé")
    
    # Charger CSV
    print(f"\n📊 Lecture du fichier : {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"   • Lignes : {len(df):,}")
    print(f"   • Colonnes : {', '.join(df.columns.tolist())}")
    
    # Définir table complète
    table_ref = f"{dataset_ref}.{table_id}"
    
    # Configuration du job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Remplace la table si existe
        autodetect=True,  # Détecte automatiquement le schéma
    )
    
    # Charger vers BigQuery
    print(f"\n⬆️  Chargement vers BigQuery...")
    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )
    
    # Attendre que le job se termine
    job.result()
    
    print(f"✅ Chargement terminé !")
    
    # Vérifier la table
    table = client.get_table(table_ref)
    
    print(f"\n📋 Informations table :")
    print(f"   • Nom complet : {table_ref}")
    print(f"   • Lignes : {table.num_rows:,}")
    print(f"   • Taille : {table.num_bytes / 1024 / 1024:.2f} MB")
    print(f"   • Créée : {table.created}")
    
    return table_ref


def main():
    """Fonction principale"""
    
    # Récupérer configuration depuis .env
    project_id = os.getenv('GCP_PROJECT_ID')
    credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
    dataset_id = os.getenv('DATASET_RAW', 'marketing_raw')
    
    print(f"📌 Configuration :")
    print(f"   • Projet : {project_id}")
    print(f"   • Dataset : {dataset_id}")
    print(f"   • Credentials : {credentials_path}")
    
    # Charger données
    table_ref = load_to_bigquery(
        csv_path='data/raw/marketing_data.csv',
        project_id=project_id,
        dataset_id=dataset_id,
        table_id='daily_performance',
        credentials_path=credentials_path
    )
    
    # Query exemple
    print(f"\n{'='*60}")
    print(f"💡 QUERY EXEMPLE")
    print(f"{'='*60}\n")
    
    print(f"Testez cette query dans BigQuery console :")
    print(f"https://console.cloud.google.com/bigquery\n")
    
    query = f"""
SELECT 
    source,
    COUNT(*) as days,
    SUM(sessions) as total_sessions,
    SUM(conversions) as total_conversions,
    SUM(revenue) as total_revenue,
    SUM(spend) as total_spend,
    ROUND(SUM(revenue) / NULLIF(SUM(spend), 0), 2) as roas
FROM `{table_ref}`
GROUP BY source
ORDER BY total_revenue DESC
"""
    
    print(query)
    
    print(f"\n{'='*60}")
    print(f"✨ SUCCÈS ! Données dans BigQuery")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()