# ==================== 2_normalisation.py ====================
"""
Sauvegardez ce contenu dans: scripts/2_normalisation.py
Nécessite: pip install fuzzywuzzy python-Levenshtein
"""

import pandas as pd
import os
import re
import logging
import numpy as np

try:
    from fuzzywuzzy import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    logging.warning("fuzzywuzzy non disponible. Installer avec: pip install fuzzywuzzy python-Levenshtein")

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class DataNormalizer:
    def __init__(self):
        self.raw_dir = "data/raw"
        self.processed_dir = "data/processed"
        os.makedirs(self.processed_dir, exist_ok=True)
    
    def clean_text(self, text):
        if pd.isna(text) or str(text).strip() == '' or str(text).lower() in ['nan', 'none', 'null']:
            return None
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        return text if text else None

    def are_duplicates(self, name1, name2, threshold=85):
        if not name1 or not name2 or not FUZZY_AVAILABLE:
            return name1 == name2 if (name1 and name2) else False
        return fuzz.ratio(name1.lower(), name2.lower()) > threshold

    def normalize_hospitals_and_places(self):
        logger.info("Normalisation Hôpitaux & Lieux...")
        dfs = []
        
        gov_path = f"{self.raw_dir}/gov_hospitals_clean.csv"
        if os.path.exists(gov_path):
            gov = pd.read_csv(gov_path, encoding='utf-8')
            required_cols = ['name', 'city', 'region', 'province', 'type', 'source']
            for col in required_cols:
                if col not in gov.columns:
                    gov[col] = None
            dfs.append(gov)

        osm_path = f"{self.raw_dir}/osm_hospitals.csv"
        if os.path.exists(osm_path):
            osm = pd.read_csv(osm_path, encoding='utf-8')
            osm['source'] = 'osm'
            for col in ['region', 'province']:
                if col not in osm.columns:
                    osm[col] = None
            dfs.append(osm)

        if not dfs:
            logger.error("Aucune donnée hôpitaux.")
            return

        df = pd.concat(dfs, ignore_index=True)
        
        target_cols = ['name', 'city', 'province', 'region', 'type', 'beds', 
                       'phone', 'email', 'website', 'latitude', 'longitude', 
                       'address', 'source']
        
        for c in target_cols:
            if c not in df.columns:
                df[c] = None
        
        for c in ['name', 'city', 'province', 'region', 'type', 'address']:
            df[c] = df[c].apply(self.clean_text)
        
        df['type'] = df['type'].fillna('Hôpital')
        
        # Compléter région/province
        city_info = {}
        for _, row in df[df['region'].notna()].iterrows():
            if row['city']:
                if row['city'] not in city_info:
                    city_info[row['city']] = {
                        'region': row['region'],
                        'province': row['province']
                    }
        
        for idx, row in df.iterrows():
            if row['city'] in city_info:
                if pd.isna(row['region']):
                    df.at[idx, 'region'] = city_info[row['city']]['region']
                if pd.isna(row['province']):
                    df.at[idx, 'province'] = city_info[row['city']]['province']
        
        # Places
        places = df[['region', 'province', 'city']].drop_duplicates(subset=['city'])
        places = places.dropna(subset=['city'])
        places['id'] = range(1, len(places) + 1)
        places.to_csv(f"{self.processed_dir}/places.csv", index=False, encoding='utf-8-sig')
        logger.info(f"✓ {len(places)} lieux.")

        place_map = places.set_index('city')['id'].to_dict()
        df['place_id'] = df['city'].map(place_map)
        
        # Déduplication
        df = df.dropna(subset=['name'])
        to_remove = set()
        df_sorted = df.sort_values('source', ascending=False)
        
        for i, row1 in df_sorted.iterrows():
            if i in to_remove:
                continue
            for j, row2 in df_sorted.iterrows():
                if j <= i or j in to_remove:
                    continue
                if (row1['city'] == row2['city'] and 
                    self.are_duplicates(row1['name'], row2['name'])):
                    if row1.notna().sum() >= row2.notna().sum():
                        to_remove.add(j)
                    else:
                        to_remove.add(i)
                        break
        
        df = df[~df.index.isin(to_remove)]
        logger.info(f"✓ Supprimé {len(to_remove)} doublons")
        
        df = df.reset_index(drop=True)
        df['id'] = range(1, len(df) + 1)
        
        df[target_cols + ['id', 'place_id']].to_csv(
            f"{self.processed_dir}/hospitals.csv", 
            index=False, encoding='utf-8-sig'
        )
        logger.info(f"✓ {len(df)} hôpitaux normalisés.")

    def normalize_equipment(self):
        logger.info("Normalisation Équipements...")
        dfs = []
        
        # 1. Charger les équipements de référence (Prioritaire)
        if os.path.exists(f"{self.raw_dir}/equipment_ref.csv"):
            dfs.append(pd.read_csv(f"{self.raw_dir}/equipment_ref.csv", encoding='utf-8'))
        
        # 2. Charger et nettoyer les dispositifs médicaux
        if os.path.exists(f"{self.raw_dir}/medical_devices.csv"):
            df_dev = pd.read_csv(f"{self.raw_dir}/medical_devices.csv", encoding='utf-8')
            
            if 'name' in df_dev.columns:
                # --- FILTRAGE ---
                df_dev['name'] = df_dev['name'].apply(self.clean_text)
                df_dev = df_dev.dropna(subset=['name'])

                # Critères d'exclusion : codes alphanumériques, chaines courtes, mots clés génériques
                mask_too_short = df_dev['name'].str.len() < 5
                mask_is_code = df_dev['name'].str.match(r'^[A-Z]{2}\d+$', na=False)
                blacklist = ['Code CNOPS', 'Dispositif Médical', 'DCM']
                mask_blacklist = df_dev['name'].isin(blacklist)

                df_clean = df_dev[~(mask_too_short | mask_is_code | mask_blacklist)]
                if not df_clean.empty:
                    dfs.append(df_clean)
        
        if not dfs:
            logger.warning("Aucun équipement valide trouvé.")
            return
        
        full_eq = pd.concat(dfs, ignore_index=True)
        full_eq['name'] = full_eq['name'].apply(self.clean_text)
        full_eq = full_eq.dropna(subset=['name'])
        
        if 'category' not in full_eq.columns:
            full_eq['category'] = 'Standard'
        full_eq['category'] = full_eq['category'].fillna('Autre équipement')
        
        if 'code' not in full_eq.columns:
            full_eq['code'] = None
        
        full_eq = full_eq.drop_duplicates(subset=['name'], keep='first')
        full_eq['id'] = range(1, len(full_eq) + 1)
        
        full_eq[['id', 'name', 'code', 'category']].to_csv(
            f"{self.processed_dir}/equipment.csv", 
            index=False, encoding='utf-8-sig'
        )
        logger.info(f"✓ {len(full_eq)} équipements valides.")

    def normalize_medications(self):
        logger.info("Normalisation Médicaments...")
        path = f"{self.raw_dir}/medicaments_clean.csv"
        if not os.path.exists(path):
            return
        
        df = pd.read_csv(path, encoding='utf-8')
        
        rename_map = {
            'SPECIALITE': 'name', 'SUBSTANCE ACTIVE': 'active_substance',
            'FORME': 'form', 'PRESENTATION': 'presentation', 'DOSAGE': 'dosage',
            'EPI': 'manufacturer', 'PPV': 'price_public', 'PH': 'price_hospital',
            'CLASSE THERAPEUTIQUE': 'therapeutic_class',
            'STATUT COMMERCIALISATION': 'commercialization_status'
        }
        
        df = df.rename(columns=rename_map)
        
        # CORRECTION DOUBLONS DE COLONNES
        df = df.loc[:, ~df.columns.duplicated()]

        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(self.clean_text)
        
        cols = [c for c in rename_map.values() if c in df.columns]
        df = df[cols].dropna(subset=['name']).drop_duplicates(subset=['name'])
        df['id'] = range(1, len(df) + 1)
        
        df.to_csv(f"{self.processed_dir}/medications.csv", index=False, encoding='utf-8-sig')
        logger.info(f"✓ {len(df)} médicaments.")

    def normalize_suppliers(self):
        logger.info("Normalisation Fournisseurs...")
        path = f"{self.raw_dir}/suppliers_consolidated.csv"
        if not os.path.exists(path):
            return
        
        df = pd.read_csv(path, encoding='utf-8')
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(self.clean_text)
        
        df = df.dropna(subset=['name']).drop_duplicates(subset=['name'])
        df['id'] = range(1, len(df) + 1)
        df.to_csv(f"{self.processed_dir}/suppliers.csv", index=False, encoding='utf-8-sig')
        logger.info(f"✓ {len(df)} fournisseurs.")

    def normalize_services(self):
        logger.info("Normalisation Services...")
        path = f"{self.raw_dir}/services_ref.csv"
        if not os.path.exists(path):
            return
        
        df = pd.read_csv(path, encoding='utf-8')
        df['name'] = df['name'].apply(self.clean_text)
        df = df.dropna(subset=['name']).drop_duplicates(subset=['name'])
        
        if 'description' not in df.columns:
            df['description'] = None
        
        df['id'] = range(1, len(df) + 1)
        df[['id', 'name', 'description']].to_csv(
            f"{self.processed_dir}/services.csv", 
            index=False, encoding='utf-8-sig'
        )
        logger.info(f"✓ {len(df)} services.")

    def generate_supplier_links(self):
        logger.info("Génération des liens Fournisseurs-Médicaments...")
        
        meds_path = f"{self.processed_dir}/medications.csv"
        supp_path = f"{self.processed_dir}/suppliers.csv"
        
        if not os.path.exists(meds_path) or not os.path.exists(supp_path):
            return

        df_meds = pd.read_csv(meds_path)
        df_supp = pd.read_csv(supp_path)
        
        links = []
        
        for _, med in df_meds.iterrows():
            if pd.isna(med.get('manufacturer')):
                continue
                
            manufacturer = str(med['manufacturer']).lower()
            
            # Recherche simple : si le nom du fabricant est contenu dans le nom du fournisseur
            match = df_supp[df_supp['name'].str.lower().str.contains(manufacturer, regex=False)]
            
            if not match.empty:
                supplier_id = match.iloc[0]['id']
                links.append({
                    'supplier_id': supplier_id,
                    'medication_id': med['id']
                })
        
        if links:
            pd.DataFrame(links).to_csv(f"{self.processed_dir}/supplier_medications.csv", index=False)
            logger.info(f"✓ Généré {len(links)} liens Fournisseur -> Médicament")
        else:
            logger.info("Aucun lien fournisseur-médicament trouvé.")

    def run(self):
        logger.info("=== PHASE 2: NORMALISATION ===")
        self.normalize_hospitals_and_places()
        self.normalize_equipment()
        self.normalize_medications()
        self.normalize_suppliers()
        self.normalize_services()
        self.generate_supplier_links() # Nouvelle étape
        logger.info("=== PHASE 2 TERMINÉE ===\n")

if __name__ == "__main__":
    DataNormalizer().run()