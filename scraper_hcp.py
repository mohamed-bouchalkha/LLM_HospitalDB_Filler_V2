"""
Scraper pour HCP.ma - Haut Commissariat au Plan
================================================
Scrape les indicateurs de santé et personnes à besoins spécifiques.

Installation:
pip install requests beautifulsoup4 pandas openpyxl lxml
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

class HCPScraper:
    """Scraper pour les indicateurs de santé du HCP"""
    
    def __init__(self, output_dir='hcp_sante'):
        self.base_url = "https://www.hcp.ma"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Créer dossiers
        os.makedirs(f'{output_dir}/indicateurs_sante', exist_ok=True)
        os.makedirs(f'{output_dir}/handicap', exist_ok=True)
        os.makedirs(f'{output_dir}/nutrition', exist_ok=True)
        os.makedirs(f'{output_dir}/publications', exist_ok=True)
        os.makedirs(f'{output_dir}/metadata', exist_ok=True)
        
        logging.info(f"✓ Dossiers créés dans: {output_dir}")
    
    def scrape_indicateurs_sante(self):
        """Scrape les indicateurs santé et personnes à besoins spécifiques"""
        url = f"{self.base_url}/Indicateurs-Sante-et-personnes-a-besoins-specifiques_r591.html"
        logging.info(f"Scraping indicateurs santé: {url}")
        
        indicateurs = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher tous les tableaux
            tables = soup.find_all('table')
            logging.info(f"  {len(tables)} tableaux trouvés")
            
            for i, table in enumerate(tables, 1):
                try:
                    # Parser le tableau
                    df = pd.read_html(str(table))[0]
                    
                    if len(df) > 0:
                        # Sauvegarder
                        filename_base = f'{self.output_dir}/indicateurs_sante/indicateur_{i}'
                        df.to_csv(f'{filename_base}.csv', index=False, encoding='utf-8-sig')
                        df.to_excel(f'{filename_base}.xlsx', index=False, engine='openpyxl')
                        
                        logging.info(f"  ✓ Tableau {i}: {len(df)} lignes × {len(df.columns)} colonnes")
                        
                        indicateurs.append({
                            'table_id': i,
                            'lignes': len(df),
                            'colonnes': len(df.columns),
                            'colonnes_noms': list(df.columns),
                            'fichier': filename_base
                        })
                        
                except Exception as e:
                    logging.warning(f"  ⚠ Erreur table {i}: {e}")
                    continue
            
            # Chercher aussi les liens vers documents Excel/PDF
            links = soup.find_all('a', href=True)
            documents = []
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                if any(ext in href.lower() for ext in ['.xls', '.xlsx', '.pdf']):
                    full_url = urljoin(self.base_url, href)
                    documents.append({
                        'titre': text,
                        'url': full_url,
                        'type': href.split('.')[-1].upper()
                    })
                    logging.info(f"  📄 Document trouvé: {text}")
            
            # Sauvegarder métadonnées
            metadata = {
                'url': url,
                'date_scraping': datetime.now().isoformat(),
                'tableaux': indicateurs,
                'documents': documents
            }
            
            with open(f'{self.output_dir}/metadata/indicateurs_sante.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            logging.info(f"✓ {len(indicateurs)} tableaux extraits")
            return indicateurs
            
        except Exception as e:
            logging.error(f"Erreur scraping indicateurs: {e}")
            return indicateurs
    
    def scrape_nutrition_sante(self):
        """Scrape les indicateurs nutrition & santé"""
        url = f"{self.base_url}/Indicateurs-Nutrition-sante_r486.html"
        logging.info(f"Scraping nutrition & santé: {url}")
        
        tables_data = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            tables = soup.find_all('table')
            logging.info(f"  {len(tables)} tableaux trouvés")
            
            for i, table in enumerate(tables, 1):
                try:
                    df = pd.read_html(str(table))[0]
                    
                    if len(df) > 0:
                        filename_base = f'{self.output_dir}/nutrition/nutrition_{i}'
                        df.to_csv(f'{filename_base}.csv', index=False, encoding='utf-8-sig')
                        df.to_excel(f'{filename_base}.xlsx', index=False, engine='openpyxl')
                        
                        logging.info(f"  ✓ Tableau {i}: {len(df)} lignes")
                        tables_data.append({'id': i, 'lignes': len(df)})
                        
                except Exception as e:
                    logging.warning(f"  ⚠ Erreur table {i}: {e}")
                    continue
            
            logging.info(f"✓ {len(tables_data)} tableaux nutrition extraits")
            return tables_data
            
        except Exception as e:
            logging.error(f"Erreur scraping nutrition: {e}")
            return tables_data
    
    def scrape_publications_sante(self):
        """Scrape les publications sur la santé"""
        url = f"{self.base_url}/Sante-et-personnes-a-besoins-specifiques_r589.html"
        logging.info(f"Scraping publications santé: {url}")
        
        publications = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher liens vers publications
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Filtrer les publications pertinentes
                if any(keyword in text.lower() for keyword in 
                      ['covid', 'santé', 'sante', 'handicap', 'couverture médicale', 'épidémie']):
                    
                    full_url = urljoin(self.base_url, href)
                    
                    pub = {
                        'titre': text,
                        'url': full_url,
                        'type': 'publication'
                    }
                    
                    publications.append(pub)
                    logging.info(f"  📰 Publication: {text[:60]}...")
                    
                    # Si PDF, télécharger
                    if href.lower().endswith('.pdf'):
                        self._download_file(full_url, text, 'publications')
            
            # Sauvegarder liste
            with open(f'{self.output_dir}/metadata/publications.json', 'w', encoding='utf-8') as f:
                json.dump(publications, f, ensure_ascii=False, indent=2)
            
            logging.info(f"✓ {len(publications)} publications trouvées")
            return publications
            
        except Exception as e:
            logging.error(f"Erreur scraping publications: {e}")
            return publications
    
    def scrape_indicateurs_sociaux(self):
        """Télécharge les indicateurs sociaux (PDF)"""
        # URLs des rapports annuels
        urls = [
            "https://casainvest.ma/sites/default/files/Les indicateurs sociaux du Maroc HCP 2023.pdf",
            "https://marocpme.gov.ma/wp-content/uploads/2024/04/Les-indicateurs-sociaux-du-Maroc-Edition-2024.pdf"
        ]
        
        indicateurs = []
        
        for url in urls:
            try:
                logging.info(f"Téléchargement: {url}")
                year = '2023' if '2023' in url else '2024'
                filename = f"Indicateurs_Sociaux_Maroc_{year}.pdf"
                
                self._download_file(url, filename, 'publications')
                indicateurs.append({'annee': year, 'url': url})
                
            except Exception as e:
                logging.warning(f"Erreur téléchargement {url}: {e}")
                continue
        
        return indicateurs
    
    def _download_file(self, url, titre, subfolder):
        """Télécharge un fichier"""
        try:
            safe_name = "".join(c for c in titre if c.isalnum() or c in (' ', '-', '_'))[:80]
            
            # Déterminer extension
            if url.lower().endswith('.pdf'):
                ext = '.pdf'
            elif url.lower().endswith('.xlsx'):
                ext = '.xlsx'
            elif url.lower().endswith('.xls'):
                ext = '.xls'
            else:
                ext = ''
            
            filename = f'{self.output_dir}/{subfolder}/{safe_name}{ext}'
            
            if os.path.exists(filename):
                logging.info(f"    ⊘ Déjà téléchargé")
                return filename
            
            logging.info(f"    ↓ Téléchargement...")
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logging.info(f"    ✓ Sauvegardé")
            time.sleep(2)
            return filename
            
        except Exception as e:
            logging.error(f"    ✗ Erreur téléchargement: {e}")
            return None
    
    def scrape_all(self):
        """Lance le scraping complet"""
        logging.info("="*60)
        logging.info("DÉMARRAGE SCRAPING HCP - SANTÉ")
        logging.info("="*60)
        
        rapport = {
            'date': datetime.now().isoformat(),
            'source': 'HCP - Haut Commissariat au Plan',
            'indicateurs_sante': 0,
            'indicateurs_nutrition': 0,
            'publications': 0,
            'rapports_annuels': 0
        }
        
        # 1. Indicateurs santé
        logging.info("\n1. Scraping indicateurs santé...")
        indicateurs = self.scrape_indicateurs_sante()
        rapport['indicateurs_sante'] = len(indicateurs)
        
        # 2. Nutrition
        logging.info("\n2. Scraping indicateurs nutrition...")
        nutrition = self.scrape_nutrition_sante()
        rapport['indicateurs_nutrition'] = len(nutrition)
        
        # 3. Publications
        logging.info("\n3. Scraping publications...")
        publications = self.scrape_publications_sante()
        rapport['publications'] = len(publications)
        
        # 4. Rapports annuels
        logging.info("\n4. Téléchargement rapports annuels...")
        rapports = self.scrape_indicateurs_sociaux()
        rapport['rapports_annuels'] = len(rapports)
        
        # Sauvegarder rapport
        with open(f'{self.output_dir}/rapport_hcp.json', 'w', encoding='utf-8') as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)
        
        logging.info("\n" + "="*60)
        logging.info("SCRAPING HCP TERMINÉ!")
        logging.info("="*60)
        logging.info(f"Indicateurs santé: {rapport['indicateurs_sante']}")
        logging.info(f"Indicateurs nutrition: {rapport['indicateurs_nutrition']}")
        logging.info(f"Publications: {rapport['publications']}")
        logging.info(f"Rapports annuels: {rapport['rapports_annuels']}")
        logging.info(f"\nRésultats dans: {self.output_dir}/")
        logging.info("="*60)
        
        return rapport


if __name__ == "__main__":
    scraper = HCPScraper()
    rapport = scraper.scrape_all()