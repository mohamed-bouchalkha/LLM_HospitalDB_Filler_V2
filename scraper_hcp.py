"""
Scraper pour HCP.ma - VERSION ADAPTÉE
======================================
Télécharge les Indicateurs Sociaux du Maroc et autres publications officielles.

Le HCP publie des fichiers Excel téléchargeables avec toutes les données de santé.

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
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class HCPScraper:
    """Scraper pour les publications du HCP"""
    
    def __init__(self, output_dir='hcp_donnees'):
        self.base_url = "https://www.hcp.ma"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Créer dossiers
        os.makedirs(f'{output_dir}/indicateurs_sociaux', exist_ok=True)
        os.makedirs(f'{output_dir}/sante', exist_ok=True)
        os.makedirs(f'{output_dir}/demographie', exist_ok=True)
        os.makedirs(f'{output_dir}/publications_pdf', exist_ok=True)
        os.makedirs(f'{output_dir}/fichiers_excel', exist_ok=True)
        os.makedirs(f'{output_dir}/metadata', exist_ok=True)
        
        logging.info(f"✓ Dossiers créés dans: {output_dir}")
    
    def telecharger_indicateurs_sociaux(self):
        """Télécharge les Indicateurs Sociaux du Maroc (fichiers Excel)"""
        url = f"{self.base_url}/downloads/Les-indicateurs-sociaux_t22430.html"
        logging.info(f"Scraping Indicateurs Sociaux: {url}")
        
        fichiers = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher tous les liens vers fichiers Excel et PDF
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Filtrer fichiers Excel et PDF
                if any(ext in href.lower() for ext in ['.xls', '.xlsx', '.pdf', '.zip']):
                    full_url = urljoin(self.base_url, href)
                    
                    # Déterminer année
                    year = self._extract_year(text + href)
                    
                    # Télécharger
                    if '.xls' in href.lower():
                        result = self._download_file(full_url, text, 'fichiers_excel')
                    else:
                        result = self._download_file(full_url, text, 'indicateurs_sociaux')
                    
                    if result:
                        fichiers.append({
                            'titre': text,
                            'url': full_url,
                            'annee': year,
                            'fichier': result
                        })
            
            logging.info(f"✓ {len(fichiers)} Indicateurs Sociaux téléchargés")
            return fichiers
            
        except Exception as e:
            logging.error(f"Erreur téléchargement Indicateurs Sociaux: {e}")
            return fichiers
    
    def telecharger_annuaire_statistique(self):
        """Télécharge l'Annuaire Statistique du Maroc (Excel)"""
        url = f"{self.base_url}/downloads/Annuaire-Statistique-du-Maroc-format-Excel_t22392.html"
        logging.info(f"Téléchargement Annuaire Statistique: {url}")
        
        fichiers = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher liens Excel
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                if '.xls' in href.lower() or '.zip' in href.lower():
                    full_url = urljoin(self.base_url, href)
                    result = self._download_file(full_url, text, 'fichiers_excel')
                    
                    if result:
                        fichiers.append({
                            'titre': text,
                            'url': full_url,
                            'fichier': result
                        })
            
            logging.info(f"✓ {len(fichiers)} Annuaires téléchargés")
            return fichiers
            
        except Exception as e:
            logging.error(f"Erreur Annuaire: {e}")
            return fichiers
    
    def scrape_page_telechargements(self):
        """Scrape la page principale de téléchargements"""
        url = f"{self.base_url}/downloads/"
        logging.info(f"Exploration page téléchargements: {url}")
        
        publications = {
            'sante': [],
            'demographie': [],
            'autres': []
        }
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher tous les liens
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                full_url = urljoin(self.base_url, href)
                
                # Catégoriser par mot-clé
                if any(kw in text.lower() for kw in ['santé', 'sante', 'handicap', 'mortalité', 'nutrition']):
                    publications['sante'].append({
                        'titre': text,
                        'url': full_url
                    })
                    logging.info(f"  📊 Santé: {text[:60]}...")
                    
                elif any(kw in text.lower() for kw in ['démographie', 'population', 'recensement']):
                    publications['demographie'].append({
                        'titre': text,
                        'url': full_url
                    })
                    
                # Si PDF ou Excel lié à santé, télécharger
                if (any(kw in text.lower() for kw in ['santé', 'sante', 'indicateurs sociaux']) and 
                    (href.endswith('.pdf') or '.xls' in href.lower())):
                    
                    if '.pdf' in href.lower():
                        self._download_file(full_url, text, 'publications_pdf')
                    else:
                        self._download_file(full_url, text, 'fichiers_excel')
            
            # Sauvegarder métadonnées
            with open(f'{self.output_dir}/metadata/publications_trouvees.json', 'w', encoding='utf-8') as f:
                json.dump(publications, f, ensure_ascii=False, indent=2)
            
            logging.info(f"✓ Publications santé: {len(publications['sante'])}")
            logging.info(f"✓ Publications démographie: {len(publications['demographie'])}")
            
            return publications
            
        except Exception as e:
            logging.error(f"Erreur exploration téléchargements: {e}")
            return publications
    
    def telecharger_publications_specifiques(self):
        """Télécharge des publications spécifiques importantes"""
        logging.info("Téléchargement publications spécifiques...")
        
        # URLs directes identifiées
        urls_importantes = [
            # Indicateurs Sociaux (Excel)
            f"{self.base_url}/file/241136/",  # Indicateurs Sociaux 2023
            f"{self.base_url}/file/241135/",  # Indicateurs Sociaux 2022
            
            # Publications santé
            f"{self.base_url}/file/231571/",  # Indicateurs santé reproductive
            f"{self.base_url}/file/231570/",  # Indicateurs mortalité
        ]
        
        fichiers = []
        
        for url in urls_importantes:
            try:
                logging.info(f"  Téléchargement: {url}")
                response = self.session.get(url, timeout=60, stream=True)
                response.raise_for_status()
                
                # Déterminer nom fichier depuis headers
                content_disp = response.headers.get('content-disposition', '')
                if 'filename=' in content_disp:
                    filename = content_disp.split('filename=')[1].strip('"')
                else:
                    filename = f"publication_{url.split('/')[-2]}"
                
                # Déterminer extension
                content_type = response.headers.get('content-type', '')
                if 'excel' in content_type or 'spreadsheet' in content_type:
                    if not filename.endswith(('.xls', '.xlsx')):
                        filename += '.xlsx'
                    folder = 'fichiers_excel'
                elif 'pdf' in content_type:
                    if not filename.endswith('.pdf'):
                        filename += '.pdf'
                    folder = 'publications_pdf'
                else:
                    folder = 'sante'
                
                filepath = f'{self.output_dir}/{folder}/{filename}'
                
                # Sauvegarder
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logging.info(f"    ✓ Sauvegardé: {filename}")
                fichiers.append(filepath)
                time.sleep(2)
                
            except Exception as e:
                logging.warning(f"    ⚠ Erreur {url}: {e}")
        
        logging.info(f"✓ {len(fichiers)} publications spécifiques téléchargées")
        return fichiers
    
    def _extract_year(self, text):
        """Extrait l'année d'un texte"""
        years = re.findall(r'20\d{2}', text)
        return years[-1] if years else None
    
    def _make_safe_filename(self, filename):
        """Crée un nom de fichier sécurisé"""
        safe = re.sub(r'[<>:"/\\|?*]', '_', filename)
        safe = "".join(c for c in safe if c.isalnum() or c in (' ', '-', '_', '.'))
        return safe[:150]
    
    def _download_file(self, url, titre, subfolder):
        """Télécharge un fichier"""
        try:
            safe_name = self._make_safe_filename(titre or 'document')
            
            # Déterminer extension depuis URL
            if '.xlsx' in url.lower():
                ext = '.xlsx'
            elif '.xls' in url.lower():
                ext = '.xls'
            elif '.pdf' in url.lower():
                ext = '.pdf'
            elif '.zip' in url.lower():
                ext = '.zip'
            else:
                ext = ''
            
            if not safe_name.endswith(ext) and ext:
                safe_name += ext
            
            filepath = f'{self.output_dir}/{subfolder}/{safe_name}'
            
            # Vérifier si existe
            if os.path.exists(filepath):
                logging.info(f"    ⊘ Déjà téléchargé: {safe_name}")
                return filepath
            
            logging.info(f"    ↓ Téléchargement: {safe_name}")
            
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            size_kb = os.path.getsize(filepath) // 1024
            logging.info(f"    ✓ Sauvegardé ({size_kb} KB)")
            time.sleep(2)
            
            return filepath
            
        except Exception as e:
            logging.error(f"    ✗ Erreur téléchargement: {e}")
            return None
    
    def scrape_all(self):
        """Lance le scraping complet"""
        logging.info("="*60)
        logging.info("SCRAPING HCP - DONNÉES OFFICIELLES")
        logging.info("="*60)
        
        rapport = {
            'date': datetime.now().isoformat(),
            'source': 'HCP - Haut Commissariat au Plan',
            'indicateurs_sociaux': 0,
            'annuaires': 0,
            'publications_sante': 0,
            'publications_specifiques': 0
        }
        
        # 1. Indicateurs Sociaux (Excel annuels)
        logging.info("\n1. Téléchargement Indicateurs Sociaux...")
        indicateurs = self.telecharger_indicateurs_sociaux()
        rapport['indicateurs_sociaux'] = len(indicateurs)
        
        # 2. Annuaire Statistique (Excel)
        logging.info("\n2. Téléchargement Annuaire Statistique...")
        annuaires = self.telecharger_annuaire_statistique()
        rapport['annuaires'] = len(annuaires)
        
        # 3. Explorer page téléchargements
        logging.info("\n3. Exploration page téléchargements...")
        publications = self.scrape_page_telechargements()
        rapport['publications_sante'] = len(publications.get('sante', []))
        
        # 4. Publications spécifiques
        logging.info("\n4. Téléchargement publications spécifiques...")
        specifiques = self.telecharger_publications_specifiques()
        rapport['publications_specifiques'] = len(specifiques)
        
        # Sauvegarder rapport
        with open(f'{self.output_dir}/rapport_hcp.json', 'w', encoding='utf-8') as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)
        
        logging.info("\n" + "="*60)
        logging.info("SCRAPING HCP TERMINÉ!")
        logging.info("="*60)
        logging.info(f"Indicateurs Sociaux: {rapport['indicateurs_sociaux']}")
        logging.info(f"Annuaires: {rapport['annuaires']}")
        logging.info(f"Publications santé: {rapport['publications_sante']}")
        logging.info(f"Publications spécifiques: {rapport['publications_specifiques']}")
        logging.info(f"\nRésultats dans: {self.output_dir}/")
        logging.info("="*60)
        
        return rapport


if __name__ == "__main__":
    scraper = HCPScraper()
    rapport = scraper.scrape_all()