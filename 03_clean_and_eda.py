# 03_clean_and_eda.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os

INPUT_CSV = "donnees_extraites/hopitaux/db_hopitaux_final.csv"
OUTPUT_REPORT = "donnees_extraites/hopitaux/eda_report.png"

def clean_data(df):
    print("--- CLEANING DATA ---")
    initial_len = len(df)
    
    # 1. Standardize City Names (Simple example)
    if 'ville' in df.columns:
        df['ville'] = df['ville'].str.title().str.strip()
        df['ville'] = df['ville'].replace({
            'Casa': 'Casablanca',
            'Rabat-Sale': 'Rabat',
            'Fes': 'Fès'
        })
    
    # 2. Deduplicate based on Name and City
    df = df.drop_duplicates(subset=['nom', 'ville'], keep='first')
    print(f"Removed {initial_len - len(df)} duplicates.")
    
    # 3. Handle numeric columns (Bed capacity)
    if 'lits' in df.columns:
        df['lits'] = pd.to_numeric(df['lits'], errors='coerce').fillna(0)
        
    return df

def generate_eda(df):
    print("--- GENERATING EDA ---")
    
    plt.figure(figsize=(15, 10))
    
    # Chart 1: Top 10 Cities by Number of Hospitals
    plt.subplot(2, 2, 1)
    if 'ville' in df.columns:
        top_cities = df['ville'].value_counts().head(10)
        sns.barplot(x=top_cities.values, y=top_cities.index, palette="viridis")
        plt.title('Top 10 Cities by Hospital Count')
    
    # Chart 2: Total Beds per Hospital Type
    plt.subplot(2, 2, 2)
    if 'type' in df.columns and 'lits' in df.columns:
        beds_by_type = df.groupby('type')['lits'].sum().sort_values(ascending=False).head(8)
        sns.barplot(x=beds_by_type.values, y=beds_by_type.index, palette="magma")
        plt.title('Total Beds by Facility Type')

    # Chart 3: Medical Staff Distribution (if columns exist from normalization)
    plt.subplot(2, 2, 3)
    # Note: Column names depend on how json_normalize flattened the JSON in script 2
    # Likely names: 'personnel.medecins', 'personnel.infirmiers'
    staff_cols = [c for c in df.columns if 'personnel' in c]
    if staff_cols:
        staff_sums = df[staff_cols].sum().sort_values(ascending=False).head(5)
        # Clean labels
        labels = [c.replace('personnel.', '') for c in staff_sums.index]
        plt.pie(staff_sums, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.title('Staff Distribution (Identified)')
    else:
        plt.text(0.5, 0.5, 'No Staff Data Found', ha='center')

    # Chart 4: Equipment Availability (Scanner/IRM)
    plt.subplot(2, 2, 4)
    equip_cols = [c for c in df.columns if 'equipements' in c]
    if equip_cols:
        equip_sums = df[equip_cols].sum().sort_values(ascending=False).head(5)
        labels = [c.replace('equipements.', '') for c in equip_sums.index]
        sns.barplot(x=equip_sums.values, y=labels, palette="rocket")
        plt.title('Key Medical Equipment Count')
    else:
        plt.text(0.5, 0.5, 'No Equipment Data Found', ha='center')
        
    plt.tight_layout()
    plt.savefig(OUTPUT_REPORT)
    print(f"Report saved to {OUTPUT_REPORT}")

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"File {INPUT_CSV} not found. Run extraction script first.")
        return
        
    df = pd.read_csv(INPUT_CSV)
    df_clean = clean_data(df)
    
    # Save cleaned version
    df_clean.to_csv(INPUT_CSV.replace(".csv", "_cleaned.csv"), index=False)
    
    generate_eda(df_clean)

if __name__ == "__main__":
    main()