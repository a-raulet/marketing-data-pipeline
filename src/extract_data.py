"""
Data Extraction Script
Génère des données marketing simulées pour le pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_marketing_data(
    start_date='2024-09-01',
    end_date='2024-11-23',
    sources=['Google Ads', 'Facebook Ads', 'LinkedIn', 'Email', 'Direct']
):
    """
    Génère données marketing simulées réalistes
    
    Args:
        start_date: Date de début (format YYYY-MM-DD)
        end_date: Date de fin (format YYYY-MM-DD)
        sources: Liste des canaux marketing
        
    Returns:
        DataFrame avec colonnes : date, source, sessions, conversions, revenue, spend
    """
    
    print(f"🎲 Génération de données : {start_date} à {end_date}")
    
    # Générer range de dates
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Seed pour reproductibilité
    np.random.seed(42)
    
    data = []
    
    for date in dates:
        for source in sources:
            # Paramètres par source (réalistes)
            if source == 'Google Ads':
                avg_sessions = 1500
                avg_conv_rate = 0.05
                avg_spend = 3000
                avg_revenue_per_conv = 150
            elif source == 'Facebook Ads':
                avg_sessions = 1200
                avg_conv_rate = 0.04
                avg_spend = 2500
                avg_revenue_per_conv = 120
            elif source == 'LinkedIn':
                avg_sessions = 400
                avg_conv_rate = 0.06
                avg_spend = 1500
                avg_revenue_per_conv = 200
            elif source == 'Email':
                avg_sessions = 800
                avg_conv_rate = 0.08
                avg_spend = 500
                avg_revenue_per_conv = 100
            else:  # Direct
                avg_sessions = 2000
                avg_conv_rate = 0.03
                avg_spend = 0
                avg_revenue_per_conv = 80
            
            # Ajouter saisonnalité (weekend lower traffic)
            weekday_factor = 0.7 if date.weekday() >= 5 else 1.0
            
            # Générer métriques avec variabilité
            sessions = int(np.random.poisson(avg_sessions * weekday_factor))
            conversions = int(np.random.binomial(sessions, avg_conv_rate))
            spend = np.random.normal(avg_spend, avg_spend * 0.2) if avg_spend > 0 else 0
            revenue = conversions * np.random.normal(avg_revenue_per_conv, avg_revenue_per_conv * 0.3)
            
            # Éviter valeurs négatives
            spend = max(0, spend)
            revenue = max(0, revenue)
            
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'source': source,
                'sessions': sessions,
                'conversions': conversions,
                'revenue': round(revenue, 2),
                'spend': round(spend, 2)
            })
    
    df = pd.DataFrame(data)
    
    print(f"✅ Généré {len(df):,} lignes de données")
    
    return df


def main():
    """Fonction principale"""
    
    print("\n" + "="*60)
    print("🚀 EXTRACTION DE DONNÉES MARKETING")
    print("="*60 + "\n")
    
    # Générer données
    df = generate_marketing_data()
    
    # Statistiques
    print(f"\n📊 Résumé des données :")
    print(f"   • Date range : {df['date'].min()} à {df['date'].max()}")
    print(f"   • Nombre de jours : {df['date'].nunique()}")
    print(f"   • Sources : {', '.join(df['source'].unique())}")
    print(f"   • Total lignes : {len(df):,}")
    
    # Aperçu
    print(f"\n📋 Aperçu des données :")
    print(df.head(10).to_string())
    
    # Créer dossier data/raw s'il n'existe pas
    output_dir = 'data/raw'
    os.makedirs(output_dir, exist_ok=True)
    
    # Sauvegarder
    output_path = os.path.join(output_dir, 'marketing_data.csv')
    df.to_csv(output_path, index=False)
    
    print(f"\n💾 Données sauvegardées : {output_path}")
    
    # Stats globales
    print(f"\n📈 Statistiques globales :")
    print(f"   • Total Sessions : {df['sessions'].sum():,}")
    print(f"   • Total Conversions : {df['conversions'].sum():,}")
    print(f"   • Total Revenue : ${df['revenue'].sum():,.2f}")
    print(f"   • Total Spend : ${df['spend'].sum():,.2f}")
    
    # Calculer ROAS global
    total_revenue = df['revenue'].sum()
    total_spend = df['spend'].sum()
    roas = total_revenue / total_spend if total_spend > 0 else 0
    
    print(f"   • ROAS Global : {roas:.2f}x")
    
    print(f"\n✨ Extraction terminée avec succès !")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()