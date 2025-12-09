# ==================== 4_import_mysql.py ====================
"""
4_import_mysql.py - VERSION FINALE ET COMPLÈTE
Importe les CSV enrichis dans la base de données MySQL locale (XAMPP).
Prend en charge les nouvelles tables de relations (stocks, fournisseurs).
"""
import pandas as pd
import pymysql
import os
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# CONFIGURATION XAMPP PAR DÉFAUT
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Vide par défaut sur XAMPP
    'database': 'morocco_health_db',
    'port': 3306
}

class MySQLImporter:
    def __init__(self):
        self.data_dir = "data/enriched"
        self.conn = None
        
        # Mapping des colonnes CSV vers colonnes MySQL
        # Format: table_name -> {csv_col: mysql_col}
        self.column_mappings = {
            'places': ['id', 'region', 'province', 'city'],
            'services': ['id', 'name', 'description'],
            'equipment': ['id', 'name', 'code', 'category'],
            'hospitals': [
                'id', 'name', 'place_id', 'address', 'type', 'beds',
                'phone', 'email', 'website', 'latitude', 'longitude', 'source'
            ],
            'medications': [
                'id', 'name', 'active_substance', 'dosage', 'form', 
                'presentation', 'therapeutic_class', 'manufacturer', 
                'price_public', 'price_hospital', 'commercialization_status'
            ],
            'suppliers': [
                'id', 'name', 'category', 'activity', 'city', 
                'address', 'phone', 'responsible_pharmacist'
            ],
            # Tables de liaison existantes
            'hospital_services': ['hospital_id', 'service_id'],
            'hospital_equipment': ['hospital_id', 'equipment_id', 'quantity'],
            
            # --- NOUVELLES TABLES DE LIAISON ---
            'hospital_medications': ['hospital_id', 'medication_id', 'stock_quantity'],
            'supplier_medications': ['supplier_id', 'medication_id'],
            'supplier_equipment': ['supplier_id', 'equipment_id']
        }

    def connect(self):
        """Connexion à MySQL et création de la DB si nécessaire"""
        try:
            temp_conn = pymysql.connect(
                host=DB_CONFIG['host'], 
                user=DB_CONFIG['user'], 
                password=DB_CONFIG['password']
            )
            temp_conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            temp_conn.close()
            
            self.conn = pymysql.connect(
                **DB_CONFIG,
                cursorclass=pymysql.cursors.DictCursor,
                local_infile=True
            )
            logger.info(f"✓ Connecté à {DB_CONFIG['database']}")
        except pymysql.MySQLError as e:
            logger.error(f"Erreur connexion MySQL: {e}")
            raise

    def run_schema(self):
        """Exécute le fichier mysql_schema.sql"""
        schema_file = "mysql_schema.sql"
        if not os.path.exists(schema_file):
            logger.error(f"Fichier {schema_file} introuvable!")
            return
            
        with open(schema_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Découpage et exécution des commandes SQL
        commands = sql_content.split(';')
        with self.conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for cmd in commands:
                cmd = cmd.strip()
                if cmd and not cmd.startswith('--'):
                    try:
                        cursor.execute(cmd)
                    except Exception as e:
                        logger.warning(f"SQL Warning: {str(e)[:100]}")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.conn.commit()
        logger.info("✓ Schéma appliqué.")

    def clean_dataframe(self, df, table_name):
        """Nettoie le DataFrame pour ne garder que les colonnes valides"""
        if table_name not in self.column_mappings:
            logger.warning(f"Pas de mapping défini pour {table_name}, utilisation de toutes les colonnes")
            return df
        
        valid_columns = self.column_mappings[table_name]
        
        # Garder uniquement les colonnes qui existent dans le CSV ET dans le mapping
        available_columns = [col for col in valid_columns if col in df.columns]
        
        if not available_columns:
            logger.error(f"Aucune colonne valide trouvée pour {table_name}")
            return pd.DataFrame()
        
        # Créer un nouveau DataFrame avec uniquement les colonnes valides
        cleaned_df = df[available_columns].copy()
        
        # Remplacer NaN par None pour MySQL
        cleaned_df = cleaned_df.replace({np.nan: None})
        
        return cleaned_df

    def import_table(self, csv_file, table_name):
        """Importe un fichier CSV dans une table MySQL"""
        path = f"{self.data_dir}/{csv_file}"
        if not os.path.exists(path):
            logger.warning(f"⚠ Fichier manquant : {csv_file}")
            return

        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception as e:
            logger.error(f"Erreur lecture {csv_file}: {e}")
            return
        
        # Nettoyer le DataFrame
        df = self.clean_dataframe(df, table_name)
        
        if df.empty:
            logger.warning(f"⚠ Aucune donnée à importer pour {table_name}")
            return

        # Construction de la requête INSERT dynamique
        cols = ",".join([f"`{k}`" for k in df.columns])
        placeholders = ",".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
        
        data = [tuple(row) for row in df.to_numpy()]
        
        try:
            with self.conn.cursor() as cursor:
                # Désactiver les checks pour la vitesse
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("SET UNIQUE_CHECKS = 0")
                
                # Import par lots pour de meilleures performances
                batch_size = 1000
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    cursor.executemany(sql, batch)
                
                cursor.execute("SET UNIQUE_CHECKS = 1")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            self.conn.commit()
            logger.info(f"✓ Importé {len(data)} lignes dans {table_name}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"✗ Erreur import {table_name}: {e}")

    def verify_imports(self):
        """Vérifie que les données ont bien été importées"""
        tables = list(self.column_mappings.keys())
        
        logger.info("\n=== VÉRIFICATION DES IMPORTS ===")
        with self.conn.cursor() as cursor:
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM `{table}`")
                    result = cursor.fetchone()
                    count = result['count'] if result else 0
                    status = "✓" if count > 0 else "⚠"
                    logger.info(f"{status} {table}: {count} enregistrements")
                except Exception as e:
                    logger.warning(f"✗ Impossible de vérifier {table}: {e}")

    def run(self):
        """Exécute le processus complet d'import"""
        logger.info("=== IMPORT MYSQL ===")
        
        try:
            self.connect()
            self.run_schema()
            
            # Ordre d'import (respecter les clés étrangères)
            logger.info("\n=== IMPORT DES TABLES DE RÉFÉRENCE ===")
            self.import_table("places.csv", "places")
            self.import_table("services.csv", "services")
            self.import_table("equipment.csv", "equipment")
            self.import_table("medications.csv", "medications")
            self.import_table("suppliers.csv", "suppliers")
            
            logger.info("\n=== IMPORT DE LA TABLE PRINCIPALE ===")
            self.import_table("hospitals.csv", "hospitals")
            
            logger.info("\n=== IMPORT DES TABLES DE LIAISON ===")
            self.import_table("hospital_services.csv", "hospital_services")
            self.import_table("hospital_equipment.csv", "hospital_equipment")
            
            # --- IMPORT DES NOUVELLES RELATIONS ---
            logger.info("\n=== IMPORT DES NOUVELLES RELATIONS ===")
            self.import_table("hospital_medications.csv", "hospital_medications")
            self.import_table("supplier_medications.csv", "supplier_medications")
            self.import_table("supplier_equipment.csv", "supplier_equipment")
            
            # Vérification finale
            self.verify_imports()
            
            logger.info("\n=== IMPORT TERMINÉ AVEC SUCCÈS ===")
            
        except Exception as e:
            logger.error(f"Erreur fatale: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("✓ Connexion fermée")

if __name__ == "__main__":
    MySQLImporter().run()