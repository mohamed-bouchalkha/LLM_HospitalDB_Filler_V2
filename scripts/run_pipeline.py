"""
Pipeline Principal - Exécution Complète
Orchestre toutes les étapes du scraping à l'import MySQL
"""

import sys
import logging
from pathlib import Path

# Ajouter le dossier scripts au path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """Exécute le pipeline complet"""
    
    logger.info("\n" + "="*70)
    logger.info("PIPELINE COMPLET - HÔPITAUX MAROCAINS")
    logger.info("="*70 + "\n")
    
    try:
        # ===== ÉTAPE 1: SCRAPING =====
        logger.info("\n### ÉTAPE 1/4: SCRAPING DES SOURCES ###\n")
        from scripts.1_scraper_complet import DataLoader
        scraper = DataLoader()
        scraper.run_all()
        
        # ===== ÉTAPE 2: NORMALISATION =====
        logger.info("\n### ÉTAPE 2/4: NORMALISATION DES DONNÉES ###\n")
        from scripts.2_normalisation import DataNormalizer
        normalizer = DataNormalizer()
        normalizer.run_all()
        
        # ===== ÉTAPE 3: ENRICHISSEMENT =====
        logger.info("\n### ÉTAPE 3/4: ENRICHISSEMENT AVEC LLM ###\n")
        from scripts.3_enrichissement_llm import Enricher
        enricher = Enricher()
        enricher.run_all()
        
        # ===== ÉTAPE 4: IMPORT MYSQL =====
        logger.info("\n### ÉTAPE 4/4: IMPORT VERS MYSQL ###\n")
        
        # Demander configuration MySQL
        print("\n" + "-"*60)
        print("CONFIGURATION MYSQL")
        print("-"*60)
        
        host = input("Host MySQL [localhost]: ").strip() or "localhost"
        user = input("Utilisateur MySQL [root]: ").strip() or "root"
        password = input("Mot de passe MySQL: ").strip()
        database = input("Nom base de données [morocco_health_db]: ").strip() or "morocco_health_db"
        port = input("Port MySQL [3306]: ").strip() or "3306"
        
        config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': int(port)
        }
        
        from scripts.4_import_mysql import MySQLImporter
        importer = MySQLImporter(config)
        importer.run_full_import()
        
        # ===== TERMINÉ =====
        logger.info("\n" + "="*70)
        logger.info("✓✓✓ PIPELINE TERMINÉ AVEC SUCCÈS ✓✓✓")
        logger.info("="*70)
        
        logger.info("\nRésultats:")
        logger.info("  - Données brutes       : data/raw/")
        logger.info("  - Données normalisées  : data/processed/")
        logger.info("  - Données enrichies    : data/enriched/")
        logger.info("  - Base MySQL           : " + database)
        
        logger.info("\nAccès PHPMyAdmin:")
        logger.info("  URL: http://localhost/phpmyadmin")
        logger.info(f"  Base: {database}")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠ Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"\n✗ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()