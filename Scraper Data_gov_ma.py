"""
Scraper pour Data.gov.ma - Données de Santé
============================================
Télécharge tous les datasets de santé disponibles sur le portail open data marocain.

Installation:
pip install requests beautifulsoup4 pandas openpyxl
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import os
from datetime import datetime
from urllib.parse import urljoin
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataGovMaScraper:
    """Scraper spécialisé pour data.gov.ma"""
    
    def __init__(self, output_dir='data_gov_ma_sante'):
        self.base_url = "https://data.gov.ma"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Créer les dossiers
        os.makedirs(f'{output_dir}/datasets_csv', exist_ok=True)
        os.makedirs(f'{output_dir}/datasets_xlsx', exist_ok=True)
        os.makedirs(f'{output_dir}/metadata', exist_ok=True)
        
        logging.info(f"✓ Dossiers créés dans: {output_dir}")
    
    def get_datasets_list(self):
        """Récupère la liste des datasets de santé"""
        url = f"{self.base_url}/data/fr/group/sante"
        logging.info(f"Récupération des datasets depuis: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            datasets = []
            
            # Chercher tous les datasets (structure CKAN)
            dataset_items = soup.find_all('li', class_='dataset-item')
            
            if not dataset_items:
                # Alternative: chercher par lien
                dataset_links = soup.find_all('a', href=lambda x: x and '/dataset/' in x)
                dataset_items = [link.parent for link in dataset_links if link.parent]
            
            logging.info(f"✓ {len(dataset_items)} datasets trouvés")
            
            for item in dataset_items:
                try:
                    dataset = self._extract_dataset_info(item)
                    if dataset and dataset.get('url'):
                        datasets.append(dataset)
                        logging.info(f"  - {dataset['titre'][:60]}...")
                except Exception as e:
                    logging.warning(f"Erreur extraction dataset: {e}")
                    continue
            
            return datasets
            
        except Exception as e:
            logging.error(f"Erreur récupération liste: {e}")
            return []
    
    def _extract_dataset_info(self, item):
        """Extrait les informations d'un dataset"""
        dataset = {
            'titre': '',
            'url': '',
            'description': '',
            'organisation': '',
            'tags': [],
            'resources': []
        }
        
        # Titre et URL
        title_elem = item.find('h3', class_='dataset-heading')
        if not title_elem:
            title_elem = item.find('a', href=lambda x: x and '/dataset/' in x)
        
        if title_elem:
            if title_elem.name == 'a':
                dataset['titre'] = title_elem.get_text(strip=True)
                dataset['url'] = urljoin(self.base_url, title_elem.get('href', ''))
            else:
                link = title_elem.find('a')
                if link:
                    dataset['titre'] = link.get_text(strip=True)
                    dataset['url'] = urljoin(self.base_url, link.get('href', ''))
        
        # Description
        desc = item.find('div', class_='notes')
        if desc:
            dataset['description'] = desc.get_text(strip=True)[:200]
        
        # Organisation
        org = item.find('p', class_='dataset-organization')
        if org:
            dataset['organisation'] = org.get_text(strip=True)
        
        return dataset
    
    def get_dataset_resources(self, dataset_url):
        """Récupère les ressources (fichiers) d'un dataset"""
        logging.info(f"Récupération ressources: {dataset_url}")
        
        try:
            response = self.session.get(dataset_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            resources = []
            
            # Chercher les ressources téléchargeables
            resource_items = soup.find_all('li', class_='resource-item')
            
            for item in resource_items:
                resource = {
                    'nom': '',
                    'format': '',
                    'url': '',
                    'description': ''
                }
                
                # Nom de la ressource
                name_elem = item.find('a', class_='heading')
                if name_elem:
                    resource['nom'] = name_elem.get_text(strip=True)
                    resource['url'] = urljoin(self.base_url, name_elem.get('href', ''))
                
                # Format
                format_elem = item.find('span', class_='format-label')
                if format_elem:
                    resource['format'] = format_elem.get_text(strip=True).upper()
                
                # Description
                desc_elem = item.find('p', class_='description')
                if desc_elem:
                    resource['description'] = desc_elem.get_text(strip=True)
                
                if resource['url']:
                    resources.append(resource)
            
            logging.info(f"  ✓ {len(resources)} ressources trouvées")
            return resources
            
        except Exception as e:
            logging.error(f"Erreur récupération ressources: {e}")
            return []
    
    def download_resource(self, resource, dataset_name):
        """Télécharge une ressource (CSV, XLSX, etc.)"""
        
        if resource['format'] not in ['CSV', 'XLSX', 'XLS', 'JSON']:
            logging.info(f"  ⊘ Format {resource['format']} ignoré")
            return None
        
        try:
            # Nom de fichier sécurisé
            safe_dataset = "".join(c for c in dataset_name if c.isalnum() or c in (' ', '-', '_'))[:50]
            safe_resource = "".join(c for c in resource['nom'] if c.isalnum() or c in (' ', '-', '_'))[:50]
            
            if resource['format'] in ['CSV']:
                folder = f'{self.output_dir}/datasets_csv'
                ext = '.csv'
            elif resource['format'] in ['XLSX', 'XLS']:
                folder = f'{self.output_dir}/datasets_xlsx'
                ext = '.xlsx'
            elif resource['format'] == 'JSON':
                folder = f'{self.output_dir}/metadata'
                ext = '.json'
            
            filename = f"{folder}/{safe_dataset}_{safe_resource}{ext}"
            
            # Télécharger
            logging.info(f"  ↓ Téléchargement: {resource['nom']}")
            response = self.session.get(resource['url'], timeout=60, stream=True)
            response.raise_for_status()
            
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logging.info(f"  ✓ Sauvegardé: {filename}")
            
            # Si CSV, essayer de le lire pour vérifier
            if resource['format'] == 'CSV':
                try:
                    df = pd.read_csv(filename, encoding='utf-8', nrows=5)
                    logging.info(f"    → {len(df.columns)} colonnes détectées")
                except:
                    logging.warning(f"    ⚠ Fichier CSV non lisible (encodage?)")
            
            time.sleep(2)  # Pause pour ne pas surcharger le serveur
            return filename
            
        except Exception as e:
            logging.error(f"  ✗ Erreur téléchargement: {e}")
            return None
    
    def scrape_all(self):
        """Lance le scraping complet"""
        logging.info("="*60)
        logging.info("DÉMARRAGE SCRAPING DATA.GOV.MA - SANTÉ")
        logging.info("="*60)
        
        # 1. Récupérer la liste des datasets
        datasets = self.get_datasets_list()
        
        if not datasets:
            logging.error("Aucun dataset trouvé!")
            return
        
        # Sauvegarder la liste
        with open(f'{self.output_dir}/metadata/liste_datasets.json', 'w', encoding='utf-8') as f:
            json.dump(datasets, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n{'='*60}")
        logging.info(f"TOTAL: {len(datasets)} datasets trouvés")
        logging.info(f"{'='*60}\n")
        
        # 2. Pour chaque dataset, récupérer et télécharger les ressources
        rapport = {
            'date': datetime.now().isoformat(),
            'total_datasets': len(datasets),
            'datasets_traites': 0,
            'fichiers_telecharges': 0,
            'erreurs': 0
        }
        
        for i, dataset in enumerate(datasets, 1):
            logging.info(f"\n[{i}/{len(datasets)}] {dataset['titre']}")
            logging.info("-" * 60)
            
            try:
                # Récupérer les ressources
                resources = self.get_dataset_resources(dataset['url'])
                dataset['resources'] = resources
                
                # Télécharger chaque ressource
                for resource in resources:
                    result = self.download_resource(resource, dataset['titre'])
                    if result:
                        rapport['fichiers_telecharges'] += 1
                
                rapport['datasets_traites'] += 1
                
            except Exception as e:
                logging.error(f"Erreur traitement dataset: {e}")
                rapport['erreurs'] += 1
            
            time.sleep(3)  # Pause entre chaque dataset
        
        # Sauvegarder le rapport final
        with open(f'{self.output_dir}/rapport_scraping.json', 'w', encoding='utf-8') as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)
        
        # Sauvegarder les métadonnées complètes
        with open(f'{self.output_dir}/metadata/datasets_complets.json', 'w', encoding='utf-8') as f:
            json.dump(datasets, f, ensure_ascii=False, indent=2)
        
        logging.info("\n" + "="*60)
        logging.info("SCRAPING TERMINÉ!")
        logging.info("="*60)
        logging.info(f"Datasets traités: {rapport['datasets_traites']}/{rapport['total_datasets']}")
        logging.info(f"Fichiers téléchargés: {rapport['fichiers_telecharges']}")
        logging.info(f"Erreurs: {rapport['erreurs']}")
        logging.info(f"\nRésultats dans: {self.output_dir}/")
        logging.info("="*60)
        
        return rapport


if __name__ == "__main__":
    scraper = DataGovMaScraper()
    rapport = scraper.scrape_all()