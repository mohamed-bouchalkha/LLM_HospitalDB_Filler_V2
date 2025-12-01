"""
Scraper pour sante.gov.ma - VERSION ADAPTÉE
============================================
Scrape les vraies données disponibles sur le site du Ministère de la Santé.

SOURCES IDENTIFIÉES:
1. Publications "Santé en Chiffres" (PDFs annuels)
2. Carte Sanitaire interactive (cartesanitaire.sante.gov.ma)
3. Liste des hôpitaux par région

Installation:
pip install requests beautifulsoup4 pandas openpyxl lxml selenium webdriver-manager
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MinistereSanteScraper:
    """Scraper adapté pour le Ministère de la Santé du Maroc"""
    
    def __init__(self, output_dir='ministere_sante'):
        self.base_url = "https://www.sante.gov.ma"
        self.carte_sanitaire_url = "http://cartesanitaire.sante.gov.ma"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # Désactiver la vérification SSL pour les sites gouvernementaux avec certificats problématiques
        self.session.verify = False
        # Supprimer les warnings SSL
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Créer dossiers
        os.makedirs(f'{output_dir}/sante_en_chiffres_pdf', exist_ok=True)
        os.makedirs(f'{output_dir}/hopitaux', exist_ok=True)
        os.makedirs(f'{output_dir}/carte_sanitaire', exist_ok=True)
        os.makedirs(f'{output_dir}/autres_publications', exist_ok=True)
        os.makedirs(f'{output_dir}/metadata', exist_ok=True)
        
        logging.info(f"✓ Dossiers créés dans: {output_dir}")
    
    def scrape_sante_en_chiffres(self):
        """Télécharge les documents "Santé en Chiffres" (2016-2023)"""
        logging.info("Scraping documents 'Santé en Chiffres'...")
        
        # URLs directes des PDFs "Santé en Chiffres" identifiés
        pdfs_urls = [
            "https://www.sante.gov.ma/Documents/2024/02/Sante en chiffre 2022 VF1.pdf",
            "https://www.sante.gov.ma/Documents/2023/03/Sante en chiffre 2019 VF (1).pdf",
            "https://www.sante.gov.ma/Documents/2019/11/Santé en chiffres 2016.pdf",
            "https://sante.gov.ma/Documents/2019/11/Santé en chiffres 2017.pdf",
            "https://www.sante.gov.ma/Publications/Etudes_enquete/Documents/SanteEnChiffre2007.pdf"
        ]
        
        publications = []
        
        # 1. Télécharger les PDFs connus
        for url in pdfs_urls:
            year = self._extract_year(url)
            filename = f"Sante_en_Chiffres_{year}.pdf" if year else os.path.basename(url)
            
            result = self._download_file(url, filename, 'sante_en_chiffres_pdf')
            if result:
                publications.append({
                    'titre': f"Santé en Chiffres {year}",
                    'annee': year,
                    'url': url,
                    'fichier': result
                })
        
        # 2. Explorer la page des publications pour en trouver d'autres
        try:
            url_page = f"{self.base_url}/Publications/Etudes_enquete/Pages/default.aspx"
            logging.info(f"Exploration de: {url_page}")
            
            response = self.session.get(url_page, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher tous les liens PDF
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Filtrer les PDFs de santé
                if href.lower().endswith('.pdf'):
                    full_url = urljoin(self.base_url, href)
                    
                    # Éviter les doublons
                    if full_url not in [p['url'] for p in publications]:
                        year = self._extract_year(text) or self._extract_year(href)
                        
                        # Télécharger
                        safe_name = self._make_safe_filename(text or os.path.basename(href))
                        result = self._download_file(full_url, safe_name, 'sante_en_chiffres_pdf')
                        
                        if result:
                            publications.append({
                                'titre': text or os.path.basename(href),
                                'annee': year,
                                'url': full_url,
                                'fichier': result
                            })
        
        except Exception as e:
            logging.warning(f"Erreur exploration page publications: {e}")
        
        # Sauvegarder métadonnées
        with open(f'{self.output_dir}/metadata/sante_en_chiffres.json', 'w', encoding='utf-8') as f:
            json.dump(publications, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ {len(publications)} documents 'Santé en Chiffres' téléchargés")
        return publications
    
    def scrape_liste_hopitaux(self):
        """Scrape la liste des hôpitaux par région"""
        url = f"{self.base_url}/EtsSante/Hopitaux/Pages/default.aspx"
        logging.info(f"Scraping liste des hôpitaux: {url}")
        
        hopitaux = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Méthode 1: Chercher les tableaux
            tables = soup.find_all('table')
            
            for i, table in enumerate(tables, 1):
                try:
                    df = pd.read_html(str(table))[0]
                    
                    if len(df) > 0:
                        # Sauvegarder
                        df.to_csv(f'{self.output_dir}/hopitaux/hopitaux_table_{i}.csv', 
                                 index=False, encoding='utf-8-sig')
                        df.to_excel(f'{self.output_dir}/hopitaux/hopitaux_table_{i}.xlsx', 
                                   index=False, engine='openpyxl')
                        
                        logging.info(f"  ✓ Tableau {i}: {len(df)} lignes")
                        
                        # Convertir en liste de dictionnaires
                        for _, row in df.iterrows():
                            hopitaux.append(row.to_dict())
                            
                except Exception as e:
                    logging.warning(f"  ⚠ Erreur tableau {i}: {e}")
            
            # Méthode 2: Chercher les listes HTML
            lists = soup.find_all(['ul', 'ol'])
            for lst in lists:
                items = lst.find_all('li')
                for item in items:
                    text = item.get_text(strip=True)
                    # Filtrer les éléments pertinents
                    if len(text) > 10 and any(kw in text.lower() for kw in ['hôpital', 'hopital', 'chu', 'centre']):
                        hopitaux.append({'nom': text, 'source': 'liste_html'})
            
            # Méthode 3: Chercher les liens vers des pages d'hôpitaux
            links = soup.find_all('a', href=True)
            for link in links:
                text = link.get_text(strip=True)
                href = link.get('href', '')
                
                if any(kw in text.lower() for kw in ['hôpital', 'hopital', 'chu']) and len(text) > 5:
                    hopitaux.append({
                        'nom': text,
                        'url': urljoin(self.base_url, href),
                        'source': 'lien'
                    })
            
            # Sauvegarder
            if hopitaux:
                # Dédupliquer
                seen = set()
                hopitaux_uniques = []
                for h in hopitaux:
                    nom = h.get('nom', '')
                    if nom and nom not in seen:
                        seen.add(nom)
                        hopitaux_uniques.append(h)
                
                df_all = pd.DataFrame(hopitaux_uniques)
                df_all.to_csv(f'{self.output_dir}/hopitaux/liste_hopitaux_complete.csv', 
                             index=False, encoding='utf-8-sig')
                df_all.to_excel(f'{self.output_dir}/hopitaux/liste_hopitaux_complete.xlsx', 
                               index=False, engine='openpyxl')
                
                with open(f'{self.output_dir}/hopitaux/hopitaux.json', 'w', encoding='utf-8') as f:
                    json.dump(hopitaux_uniques, f, ensure_ascii=False, indent=2)
                
                logging.info(f"✓ {len(hopitaux_uniques)} hôpitaux extraits (après déduplication)")
                return hopitaux_uniques
            
            else:
                logging.warning("⚠ Aucun hôpital trouvé sur cette page")
                return []
            
        except Exception as e:
            logging.error(f"Erreur scraping hôpitaux: {e}")
            return []
    
    def scrape_annuaire_hopitaux(self):
        """Scrape l'annuaire des hôpitaux (ancienne version)"""
        url = f"{self.base_url}/annuaire/Hopitaux.htm"
        logging.info(f"Scraping annuaire hôpitaux: {url}")
        
        hopitaux_annuaire = []
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher les tableaux
            tables = soup.find_all('table')
            
            for i, table in enumerate(tables, 1):
                try:
                    df = pd.read_html(str(table))[0]
                    
                    if len(df) > 2:  # Ignorer les petits tableaux
                        df.to_csv(f'{self.output_dir}/hopitaux/annuaire_hopitaux_{i}.csv', 
                                 index=False, encoding='utf-8-sig')
                        
                        logging.info(f"  ✓ Annuaire tableau {i}: {len(df)} lignes")
                        
                        for _, row in df.iterrows():
                            hopitaux_annuaire.append(row.to_dict())
                
                except Exception as e:
                    logging.warning(f"  ⚠ Erreur annuaire table {i}: {e}")
            
            if hopitaux_annuaire:
                with open(f'{self.output_dir}/hopitaux/annuaire_hopitaux.json', 'w', encoding='utf-8') as f:
                    json.dump(hopitaux_annuaire, f, ensure_ascii=False, indent=2)
            
            return hopitaux_annuaire
            
        except Exception as e:
            logging.warning(f"Erreur scraping annuaire: {e}")
            return []
    
    def scrape_autres_publications(self):
        """Scrape d'autres publications importantes"""
        logging.info("Scraping autres publications...")
        
        # URLs de publications importantes identifiées
        publications_urls = [
            "https://www.sante.gov.ma/Documents/2023/11/PSNI 2024-2030.pdf",  # Plan Stratégique
            "https://www.sante.gov.ma/Documents/2021/rapport_SNFS VD avril 2021.pdf",  # Financement
            "https://www.sante.gov.ma/Documents/2023/04/SNSSR 2021-2030.pdf",  # Santé reproductive
            "https://www.sante.gov.ma/Documents/2023/2_Conférence/Documents/Revue Fr.pdf"  # Revue 2022-2023
        ]
        
        publications = []
        
        for url in publications_urls:
            filename = os.path.basename(url)
            result = self._download_file(url, filename, 'autres_publications')
            
            if result:
                publications.append({
                    'titre': filename.replace('.pdf', '').replace('_', ' '),
                    'url': url,
                    'fichier': result
                })
        
        logging.info(f"✓ {len(publications)} autres publications téléchargées")
        return publications
    
    def _extract_year(self, text):
        """Extrait l'année d'un texte"""
        years = re.findall(r'20\d{2}', text)
        return years[0] if years else None
    
    def _make_safe_filename(self, filename):
        """Crée un nom de fichier sécurisé"""
        # Garder uniquement les caractères alphanumériques et quelques symboles
        safe = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.'))
        return safe[:100]  # Limiter la longueur
    
    def _download_file(self, url, filename, subfolder):
        """Télécharge un fichier"""
        try:
            # S'assurer que le fichier a une extension
            if not filename.endswith(('.pdf', '.xlsx', '.xls', '.csv')):
                ext = url.split('.')[-1] if '.' in url else 'pdf'
                filename = f"{filename}.{ext}"
            
            filepath = f'{self.output_dir}/{subfolder}/{filename}'
            
            # Vérifier si déjà téléchargé
            if os.path.exists(filepath):
                logging.info(f"    ⊘ Déjà téléchargé: {filename}")
                return filepath
            
            logging.info(f"    ↓ Téléchargement: {filename}")
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logging.info(f"    ✓ Sauvegardé: {filepath}")
            time.sleep(2)  # Pause courtoise
            return filepath
            
        except Exception as e:
            logging.error(f"    ✗ Erreur téléchargement {filename}: {e}")
            return None
    
    def scrape_all(self):
        """Lance le scraping complet"""
        logging.info("="*60)
        logging.info("DÉMARRAGE SCRAPING MINISTÈRE DE LA SANTÉ")
        logging.info("="*60)
        
        rapport = {
            'date': datetime.now().isoformat(),
            'source': 'Ministère de la Santé et de la Protection Sociale',
            'sante_en_chiffres': 0,
            'hopitaux_liste': 0,
            'hopitaux_annuaire': 0,
            'autres_publications': 0
        }
        
        # 1. Documents "Santé en Chiffres"
        logging.info("\n1. Téléchargement 'Santé en Chiffres'...")
        sante_chiffres = self.scrape_sante_en_chiffres()
        rapport['sante_en_chiffres'] = len(sante_chiffres)
        
        # 2. Liste des hôpitaux
        logging.info("\n2. Scraping liste des hôpitaux...")
        hopitaux = self.scrape_liste_hopitaux()
        rapport['hopitaux_liste'] = len(hopitaux)
        
        # 3. Annuaire des hôpitaux
        logging.info("\n3. Scraping annuaire des hôpitaux...")
        annuaire = self.scrape_annuaire_hopitaux()
        rapport['hopitaux_annuaire'] = len(annuaire)
        
        # 4. Autres publications
        logging.info("\n4. Téléchargement autres publications...")
        autres_pubs = self.scrape_autres_publications()
        rapport['autres_publications'] = len(autres_pubs)
        
        # Sauvegarder rapport
        with open(f'{self.output_dir}/rapport_ministere.json', 'w', encoding='utf-8') as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)
        
        logging.info("\n" + "="*60)
        logging.info("SCRAPING MINISTÈRE TERMINÉ!")
        logging.info("="*60)
        logging.info(f"Documents 'Santé en Chiffres': {rapport['sante_en_chiffres']}")
        logging.info(f"Hôpitaux (liste): {rapport['hopitaux_liste']}")
        logging.info(f"Hôpitaux (annuaire): {rapport['hopitaux_annuaire']}")
        logging.info(f"Autres publications: {rapport['autres_publications']}")
        logging.info(f"\nRésultats dans: {self.output_dir}/")
        logging.info("="*60)
        
        return rapport


if __name__ == "__main__":
    scraper = MinistereSanteScraper()
    rapport = scraper.scrape_all()