# ==================== 1_scraper_complet.py ====================
"""
Sauvegardez ce contenu dans: scripts/1_scraper_complet.py
"""

import os
import json
import pandas as pd
import logging
import warnings
import re

warnings.simplefilter(action='ignore', category=UserWarning)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self.raw_dir = "data/raw"
        os.makedirs(self.raw_dir, exist_ok=True)

    def clean_text(self, text):
        if pd.isna(text) or str(text).strip() == '' or str(text).lower() in ['nan', 'none', 'null']:
            return None
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        return text if text else None

    def load_gov_hospitals(self):
        file_path = f"{self.raw_dir}/gov_hospitals.csv"
        clean_path = f"{self.raw_dir}/gov_hospitals_clean.csv"
        
        if not os.path.exists(file_path):
            logger.warning(f"Fichier introuvable : {file_path}")
            return pd.DataFrame()
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            header_row = 0
            for i, line in enumerate(lines[:20]):
                if "Région" in line or "Region" in line:
                    header_row = i
                    break
            
            df = pd.read_csv(file_path, header=header_row, encoding='utf-8')
            df.columns = df.columns.str.strip()
            
            column_mapping = {
                'Région': 'region', 'Region': 'region',
                'Delegation': 'province', 'Délégation': 'province',
                'Commune': 'city',
                'Etablissement hospitalier': 'name',
                'Établissement hospitalier': 'name',
                'Catégorie': 'type', 'Categorie': 'type'
            }
            
            df = df.rename(columns=column_mapping)
            
            for col in ['region', 'province', 'city', 'name', 'type']:
                if col in df.columns:
                    df[col] = df[col].apply(self.clean_text)
            
            df = df.dropna(how='all')
            df['source'] = 'gov'
            
            df.to_csv(clean_path, index=False, encoding='utf-8-sig')
            logger.info(f"✓ Nettoyé {len(df)} hôpitaux gouvernementaux.")
            return df
            
        except Exception as e:
            logger.error(f"Erreur gov_hospitals: {e}")
            return pd.DataFrame()

    def load_osm_hospitals(self):
        file_path = f"{self.raw_dir}/osm_hospitals.csv"
        if not os.path.exists(file_path):
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            for col in ['name', 'city', 'address', 'type']:
                if col in df.columns:
                    df[col] = df[col].apply(self.clean_text)
            
            for col in ['latitude', 'longitude']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"✓ Chargé {len(df)} hôpitaux OSM.")
            return df
        except Exception as e:
            logger.error(f"Erreur OSM: {e}")
            return pd.DataFrame()

    def convert_medicaments(self):
        json_path = f"{self.raw_dir}/medicaments.json"
        csv_path = f"{self.raw_dir}/medicaments_clean.csv"
        
        if not os.path.exists(json_path):
            return
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            valid_data = []
            for d in data:
                if not isinstance(d, dict):
                    continue
                name = d.get('SPECIALITE') or d.get('nom')
                if name and str(name).strip().lower() not in ['nan', 'none', '']:
                    valid_data.append(d)
            
            if valid_data:
                df = pd.DataFrame(valid_data)
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].apply(self.clean_text)
                
                name_col = 'SPECIALITE' if 'SPECIALITE' in df.columns else 'nom'
                df = df.drop_duplicates(subset=[name_col], keep='first')
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"✓ Converti {len(df)} médicaments.")
        except Exception as e:
            logger.error(f"Erreur medicaments: {e}")

    def convert_suppliers(self):
        suppliers = []
        
        # Dispositifs
        path_disp = f"{self.raw_dir}/dispositifs.json"
        if os.path.exists(path_disp):
            try:
                with open(path_disp, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for item in data:
                    if isinstance(item, dict):
                        name = self.clean_text(item.get('NOM'))
                        if name:
                            suppliers.append({
                                'name': name,
                                'category': 'Dispositif Médical',
                                'activity': self.clean_text(item.get('ACTIVITE')),
                                'address': self.clean_text(item.get('ADRESSE')),
                                'city': None, 'phone': None,
                                'responsible_pharmacist': None
                            })
            except: pass

        # Établissements
        path_etab = f"{self.raw_dir}/etablissements.json"
        if os.path.exists(path_etab):
            try:
                with open(path_etab, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for item in data:
                    if isinstance(item, dict):
                        name = self.clean_text(item.get('NOM'))
                        if name:
                            suppliers.append({
                                'name': name,
                                'category': 'Grossiste Pharmaceutique',
                                'activity': 'Répartition Pharmaceutique',
                                'address': self.clean_text(item.get('ADRESSE')),
                                'city': self.clean_text(item.get('VILLE')),
                                'phone': self.clean_text(item.get('TEL')),
                                'responsible_pharmacist': self.clean_text(item.get('NOM PHARMACIEN RESPONSABLE'))
                            })
            except: pass
        
        if suppliers:
            df = pd.DataFrame(suppliers)
            df = df.drop_duplicates(subset=['name'], keep='first')
            df.to_csv(f"{self.raw_dir}/suppliers_consolidated.csv", index=False, encoding='utf-8-sig')
            logger.info(f"✓ Consolidé {len(df)} fournisseurs.")

    def convert_medical_devices(self):
        xls_path = f"{self.raw_dir}/medical_devices.xlsm"
        csv_path = f"{self.raw_dir}/medical_devices.csv"
        
        if not os.path.exists(xls_path):
            return
        
        try:
            df = pd.read_excel(xls_path, engine='openpyxl')
            
            potential_name_cols = [c for c in df.columns if any(
                kw in str(c).lower() for kw in ['nom', 'description', 'device', 'materiel']
            )]
            
            if not potential_name_cols:
                potential_name_cols = [df.columns[0]]
            
            df = df.rename(columns={potential_name_cols[0]: 'name'})
            df['name'] = df['name'].apply(self.clean_text)
            df = df.dropna(subset=['name'])
            
            if 'category' not in df.columns:
                df['category'] = 'Dispositif Médical'
            
            cols = ['name', 'category']
            if 'code' in df.columns:
                cols.append('code')
            
            df[cols].drop_duplicates(subset=['name']).to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"✓ Converti medical_devices ({len(df)} lignes).")
        except Exception as e:
            logger.error(f"Erreur medical_devices: {e}")

    def run(self):
        logger.info("=== PHASE 1: SCRAPER ===")
        self.load_gov_hospitals()
        self.load_osm_hospitals()
        self.convert_medicaments()
        self.convert_suppliers()
        self.convert_medical_devices()
        logger.info("=== PHASE 1 TERMINÉE ===\n")

if __name__ == "__main__":
    DataLoader().run()