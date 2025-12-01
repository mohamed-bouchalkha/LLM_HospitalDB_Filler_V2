"""
Scraper pour AMMPS.sante.gov.ma
=================================
Scrape la base de données des médicaments et dispositifs médicaux.

NOTE IMPORTANTE: Le site AMMPS utilise JavaScript pour charger les données.
Ce script utilise Selenium pour gérer le contenu dynamique.

Installation:
pip install selenium beautifulsoup4 pandas openpyxl webdriver-manager
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import os
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class AMMPSScraper:
    """Scraper pour la base de données AMMPS"""
    
    def __init__(self, output_dir='ammps_data'):
        self.base_url = "https://ammps.sante.gov.ma"
        self.output_dir = output_dir
        
        # Créer dossiers
        os.makedirs(f'{output_dir}/medicaments', exist_ok=True)
        os.makedirs(f'{output_dir}/dispositifs_medicaux', exist_ok=True)
        os.makedirs(f'{output_dir}/etablissements', exist_ok=True)
        
        # Configuration Selenium
        self.driver = None
        
        logging.info(f"✓ Dossiers créés dans: {output_dir}")
    
    def init_driver(self):
        """Initialise le driver Selenium"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Mode sans interface
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            logging.info("✓ Driver Selenium initialisé")
            return True
            
        except Exception as e:
            logging.error(f"Erreur initialisation driver: {e}")
            return False
    
    def scrape_medicaments_list(self, max_pages=10):
        """Scrape la liste des médicaments"""
        url = f"{self.base_url}/basesdedonnes/listes-medicaments"
        logging.info(f"Scraping liste médicaments: {url}")
        
        medicaments = []
        
        try:
            self.driver.get(url)
            
            # Attendre le chargement de la page
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)  # Laisser JavaScript charger
            
            page = 1
            while page <= max_pages:
                logging.info(f"  Page {page}/{max_pages}")
                
                # Récupérer le HTML
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # Chercher les médicaments dans la page
                # Structure à ajuster selon le HTML réel
                medicament_items = soup.find_all('div', class_=['medicament-item', 'drug-item'])
                
                if not medicament_items:
                    # Essayer de trouver un tableau
                    tables = soup.find_all('table')
                    if tables:
                        for table in tables:
                            try:
                                df = pd.read_html(str(table))[0]
                                # Sauvegarder le tableau
                                df.to_csv(f'{self.output_dir}/medicaments/medicaments_page_{page}.csv', 
                                         index=False, encoding='utf-8-sig')
                                logging.info(f"    ✓ Tableau sauvegardé: {len(df)} lignes")
                                
                                # Ajouter au JSON
                                for _, row in df.iterrows():
                                    medicaments.append(row.to_dict())
                            except:
                                continue
                
                # Chercher aussi dans les listes
                items = soup.find_all(['li', 'div'], class_=lambda x: x and ('item' in x.lower() or 'drug' in x.lower()))
                
                for item in items:
                    try:
                        med = {
                            'nom': '',
                            'dci': '',
                            'forme': '',
                            'dosage': '',
                            'prix': '',
                            'statut': ''
                        }
                        
                        # Extraction des données (à ajuster selon structure)
                        text = item.get_text(strip=True)
                        if len(text) > 10:
                            med['nom'] = text[:200]
                            medicaments.append(med)
                    except:
                        continue
                
                # Essayer de passer à la page suivante
                try:
                    next_button = self.driver.find_element(By.LINK_TEXT, "Suivant")
                    if not next_button or 'disabled' in next_button.get_attribute('class'):
                        break
                    next_button.click()
                    time.sleep(3)
                    page += 1
                except:
                    logging.info("  Plus de pages disponibles")
                    break
            
            logging.info(f"✓ {len(medicaments)} médicaments extraits")
            
            # Sauvegarder
            if medicaments:
                df = pd.DataFrame(medicaments)
                df.to_csv(f'{self.output_dir}/medicaments/liste_complete_medicaments.csv', 
                         index=False, encoding='utf-8-sig')
                df.to_excel(f'{self.output_dir}/medicaments/liste_complete_medicaments.xlsx', 
                           index=False, engine='openpyxl')
                
                with open(f'{self.output_dir}/medicaments/medicaments.json', 'w', encoding='utf-8') as f:
                    json.dump(medicaments, f, ensure_ascii=False, indent=2)
            
            return medicaments
            
        except Exception as e:
            logging.error(f"Erreur scraping médicaments: {e}")
            return medicaments
    
    def scrape_dispositifs_medicaux(self):
        """Scrape les dispositifs médicaux"""
        url = f"{self.base_url}/basesdedonnes/societes-de-dispositifs-medicaux"
        logging.info(f"Scraping dispositifs médicaux: {url}")
        
        dispositifs = []
        
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)
            
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # Chercher tableaux
            tables = soup.find_all('table')
            for i, table in enumerate(tables):
                try:
                    df = pd.read_html(str(table))[0]
                    df.to_csv(f'{self.output_dir}/dispositifs_medicaux/dispositifs_{i+1}.csv', 
                             index=False, encoding='utf-8-sig')
                    logging.info(f"  ✓ Tableau {i+1}: {len(df)} lignes")
                    
                    for _, row in df.iterrows():
                        dispositifs.append(row.to_dict())
                except:
                    continue
            
            if dispositifs:
                with open(f'{self.output_dir}/dispositifs_medicaux/dispositifs.json', 'w', encoding='utf-8') as f:
                    json.dump(dispositifs, f, ensure_ascii=False, indent=2)
            
            return dispositifs
            
        except Exception as e:
            logging.error(f"Erreur scraping dispositifs: {e}")
            return dispositifs
    
    def scrape_etablissements(self):
        """Scrape les établissements pharmaceutiques"""
        url = f"{self.base_url}/basesdedonnes/etablissements-pharmaceutiques-grossistes-repartiteurs"
        logging.info(f"Scraping établissements: {url}")
        
        etablissements = []
        
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)
            
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # Chercher tableaux
            tables = soup.find_all('table')
            for i, table in enumerate(tables):
                try:
                    df = pd.read_html(str(table))[0]
                    df.to_csv(f'{self.output_dir}/etablissements/etablissements_{i+1}.csv', 
                             index=False, encoding='utf-8-sig')
                    logging.info(f"  ✓ Tableau {i+1}: {len(df)} lignes")
                    
                    for _, row in df.iterrows():
                        etablissements.append(row.to_dict())
                except:
                    continue
            
            if etablissements:
                with open(f'{self.output_dir}/etablissements/etablissements.json', 'w', encoding='utf-8') as f:
                    json.dump(etablissements, f, ensure_ascii=False, indent=2)
            
            return etablissements
            
        except Exception as e:
            logging.error(f"Erreur scraping établissements: {e}")
            return etablissements
    
    def scrape_all(self):
        """Lance le scraping complet"""
        logging.info("="*60)
        logging.info("DÉMARRAGE SCRAPING AMMPS")
        logging.info("="*60)
        
        if not self.init_driver():
            logging.error("Impossible d'initialiser le driver!")
            return
        
        try:
            rapport = {
                'date': datetime.now().isoformat(),
                'source': 'AMMPS',
                'medicaments': 0,
                'dispositifs': 0,
                'etablissements': 0
            }
            
            # 1. Médicaments
            logging.info("\n1. Scraping médicaments...")
            medicaments = self.scrape_medicaments_list(max_pages=5)
            rapport['medicaments'] = len(medicaments)
            
            # 2. Dispositifs médicaux
            logging.info("\n2. Scraping dispositifs médicaux...")
            dispositifs = self.scrape_dispositifs_medicaux()
            rapport['dispositifs'] = len(dispositifs)
            
            # 3. Établissements
            logging.info("\n3. Scraping établissements...")
            etablissements = self.scrape_etablissements()
            rapport['etablissements'] = len(etablissements)
            
            # Sauvegarder rapport
            with open(f'{self.output_dir}/rapport_ammps.json', 'w', encoding='utf-8') as f:
                json.dump(rapport, f, ensure_ascii=False, indent=2)
            
            logging.info("\n" + "="*60)
            logging.info("SCRAPING AMMPS TERMINÉ!")
            logging.info("="*60)
            logging.info(f"Médicaments: {rapport['medicaments']}")
            logging.info(f"Dispositifs: {rapport['dispositifs']}")
            logging.info(f"Établissements: {rapport['etablissements']}")
            logging.info(f"\nRésultats dans: {self.output_dir}/")
            logging.info("="*60)
            
            return rapport
            
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("✓ Driver fermé")


if __name__ == "__main__":
    scraper = AMMPSScraper()
    rapport = scraper.scrape_all()