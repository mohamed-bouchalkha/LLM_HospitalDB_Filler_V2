"""
Extracteur de Données Hospitalières avec LLM Gratuit
=====================================================
Utilise un LLM pour extraire les données structurées depuis les PDFs.

CHOIX DE LLM (du plus facile au plus puissant):
1. Groq API (GRATUIT, RAPIDE) - Recommandé
2. Ollama (LOCAL, GRATUIT)
3. LM Studio (LOCAL, GRATUIT)

Installation:
pip install PyPDF2 pdfplumber groq ollama pandas openpyxl pillow pytesseract
"""

import os
import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Dict, Any

# PDF Processing
import PyPDF2
import pdfplumber

# LLM APIs
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    print("⚠️  Groq non installé. Installez avec: pip install groq")

try:
    import ollama
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class HospitalDataExtractor:
    """Extracteur intelligent de données hospitalières avec LLM"""
    
    def __init__(self, 
                 input_dir='ministere_sante',
                 output_dir='donnees_extraites',
                 llm_provider='groq',
                 groq_api_key=None):
        
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.llm_provider = llm_provider
        
        # Créer dossiers de sortie
        (self.output_dir / 'hopitaux').mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'medecins').mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'statistiques').mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'metadata').mkdir(parents=True, exist_ok=True)
        
        # Configurer LLM
        self.llm_client = None
        self._setup_llm(groq_api_key)
        
        logging.info(f"✓ Extracteur initialisé avec {llm_provider.upper()}")
    
    def _setup_llm(self, groq_api_key):
        """Configure le client LLM"""
        
        if self.llm_provider == 'groq':
            if not GROQ_AVAILABLE:
                raise Exception("Groq non installé. Exécutez: pip install groq")
            
            # Utiliser clé API ou variable d'environnement
            api_key = groq_api_key or os.getenv('GROQ_API_KEY')
            
            if not api_key:
                print("\n" + "="*60)
                print("CONFIGURATION GROQ API")
                print("="*60)
                print("1. Créez un compte gratuit sur: https://console.groq.com")
                print("2. Générez une clé API")
                print("3. Ajoutez-la ici ou dans .env: GROQ_API_KEY=votre_clé")
                print("="*60 + "\n")
                api_key = input("Entrez votre clé API Groq: ").strip()
            
            self.llm_client = Groq(api_key=api_key)
            self.model_name = "llama-3.3-70b-versatile"  # Modèle gratuit puissant
            logging.info("✓ Groq API configuré")
        
        elif self.llm_provider == 'ollama':
            if not OLLAMA_AVAILABLE:
                raise Exception("Ollama non installé. Téléchargez: https://ollama.ai")
            
            # Vérifier qu'Ollama est lancé
            try:
                ollama.list()
                self.model_name = "llama3.2"  # ou mistral, phi3
                logging.info("✓ Ollama configuré")
            except Exception as e:
                raise Exception(f"Ollama non lancé. Démarrez avec: ollama serve\n{e}")
        
        else:
            raise ValueError(f"LLM provider '{self.llm_provider}' non supporté")
    
    def extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Extrait le texte d'un PDF"""
        logging.info(f"Extraction texte: {pdf_path.name}")
        
        text = ""
        
        # Méthode 1: pdfplumber (meilleure qualité)
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"\n--- Page {i+1} ---\n{page_text}"
            
            if text.strip():
                logging.info(f"  ✓ {len(text)} caractères extraits (pdfplumber)")
                return text
        except Exception as e:
            logging.warning(f"  ⚠ pdfplumber échoué: {e}")
        
        # Méthode 2: PyPDF2 (fallback)
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for i, page in enumerate(pdf_reader.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"\n--- Page {i+1} ---\n{page_text}"
            
            logging.info(f"  ✓ {len(text)} caractères extraits (PyPDF2)")
        except Exception as e:
            logging.error(f"  ✗ Extraction échouée: {e}")
        
        return text
    
    def create_extraction_prompt(self, text: str, data_type: str) -> str:
        """Crée le prompt pour le LLM selon le type de données"""
        
        prompts = {
            'hopitaux': """Tu es un expert en extraction de données hospitalières. 

Analyse ce texte et extrait UNIQUEMENT les informations sur les hôpitaux du Maroc.

Pour chaque hôpital, extrais:
- Nom complet de l'hôpital
- Ville/Région
- Type (CHU, Hôpital Régional, Centre de Santé, etc.)
- Nombre de lits (si mentionné)
- Spécialités (si mentionné)
- Capacité/Services (si mentionné)

Réponds UNIQUEMENT avec un JSON valide, sans commentaires:
{
  "hopitaux": [
    {
      "nom": "...",
      "ville": "...",
      "region": "...",
      "type": "...",
      "nombre_lits": 0,
      "specialites": ["..."],
      "services": ["..."]
    }
  ]
}

Si aucun hôpital n'est trouvé, réponds: {"hopitaux": []}

TEXTE À ANALYSER:
""",
            
            'medecins': """Tu es un expert en extraction de données médicales.

Analyse ce texte et extrait les statistiques sur les médecins et le personnel de santé au Maroc.

Extrais:
- Nombre total de médecins
- Médecins par région/ville
- Médecins par spécialité
- Ratio médecins/habitants
- Pharmaciens
- Infirmiers
- Autres personnels de santé

Réponds UNIQUEMENT avec un JSON valide:
{
  "statistiques_medecins": {
    "total_medecins": 0,
    "par_region": {},
    "par_specialite": {},
    "ratio_habitants": "",
    "pharmaciens": 0,
    "infirmiers": 0
  }
}

TEXTE À ANALYSER:
""",
            
            'statistiques_generales': """Tu es un expert en analyse de données de santé publique.

Analyse ce texte et extrait TOUTES les statistiques de santé importantes pour le Maroc.

Extrais:
- Indicateurs de santé (taux de mortalité, espérance de vie, etc.)
- Budget/Dépenses de santé
- Infrastructures sanitaires (nombre d'hôpitaux, centres, etc.)
- Couverture médicale
- Programmes de santé
- Chiffres clés par année

Réponds UNIQUEMENT avec un JSON valide:
{
  "statistiques": {
    "annee": "",
    "population": 0,
    "infrastructures": {},
    "personnel": {},
    "budget": {},
    "indicateurs_sante": {},
    "programmes": []
  }
}

TEXTE À ANALYSER:
"""
        }
        
        base_prompt = prompts.get(data_type, prompts['statistiques_generales'])
        
        # Limiter la taille du texte (max 15000 caractères pour éviter dépassement tokens)
        if len(text) > 15000:
            text = text[:15000] + "\n... [TEXTE TRONQUÉ] ..."
        
        return base_prompt + text
    
    def query_llm(self, prompt: str) -> str:
        """Interroge le LLM"""
        
        try:
            if self.llm_provider == 'groq':
                response = self.llm_client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {
                            "role": "system",
                            "content": "Tu es un expert en extraction de données structurées. Tu réponds UNIQUEMENT en JSON valide, sans texte additionnel."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    temperature=0.1,
                    max_tokens=4000
                )
                return response.choices[0].message.content
            
            elif self.llm_provider == 'ollama':
                response = ollama.chat(
                    model=self.model_name,
                    messages=[
                        {
                            'role': 'system',
                            'content': 'Tu réponds uniquement en JSON valide.'
                        },
                        {
                            'role': 'user',
                            'content': prompt
                        }
                    ]
                )
                return response['message']['content']
        
        except Exception as e:
            logging.error(f"Erreur LLM: {e}")
            return "{}"
    
    def extract_json_from_response(self, response: str) -> Dict:
        """Extrait le JSON de la réponse du LLM"""
        
        # Nettoyer la réponse
        response = response.strip()
        
        # Chercher JSON entre ``` ou directement
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            response = json_match.group(1)
        
        # Parser JSON
        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            logging.warning(f"JSON invalide: {e}")
            # Essayer de réparer les erreurs communes
            response = response.replace("'", '"')
            response = re.sub(r',\s*}', '}', response)
            response = re.sub(r',\s*]', ']', response)
            
            try:
                return json.loads(response)
            except:
                logging.error("Impossible de parser le JSON")
                return {}
    
    def process_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """Traite un PDF complet"""
        logging.info(f"\n{'='*60}")
        logging.info(f"Traitement: {pdf_path.name}")
        logging.info(f"{'='*60}")
        
        # 1. Extraire texte
        text = self.extract_text_from_pdf(pdf_path)
        
        if not text.strip():
            logging.warning("Aucun texte extrait du PDF")
            return {}
        
        results = {
            'fichier': pdf_path.name,
            'date_traitement': datetime.now().isoformat(),
            'donnees_extraites': {}
        }
        
        # 2. Extraire différents types de données
        data_types = ['hopitaux', 'medecins', 'statistiques_generales']
        
        for data_type in data_types:
            logging.info(f"\n  Extraction: {data_type}...")
            
            prompt = self.create_extraction_prompt(text, data_type)
            response = self.query_llm(prompt)
            data = self.extract_json_from_response(response)
            
            if data:
                results['donnees_extraites'][data_type] = data
                logging.info(f"  ✓ Données extraites pour {data_type}")
            else:
                logging.warning(f"  ⚠ Aucune donnée pour {data_type}")
        
        return results
    
    def process_all_pdfs(self):
        """Traite tous les PDFs trouvés"""
        logging.info("="*60)
        logging.info("EXTRACTION DONNÉES AVEC LLM")
        logging.info("="*60)
        
        # Trouver tous les PDFs
        pdf_files = list(self.input_dir.rglob('*.pdf'))
        
        if not pdf_files:
            logging.error(f"Aucun PDF trouvé dans {self.input_dir}")
            return
        
        logging.info(f"\n{len(pdf_files)} PDFs trouvés\n")
        
        all_results = []
        all_hopitaux = []
        all_statistiques = []
        
        for i, pdf_path in enumerate(pdf_files, 1):
            logging.info(f"\n[{i}/{len(pdf_files)}] {pdf_path.name}")
            
            try:
                result = self.process_pdf(pdf_path)
                
                if result:
                    all_results.append(result)
                    
                    # Agréger données
                    donnees = result.get('donnees_extraites', {})
                    
                    if 'hopitaux' in donnees:
                        hopitaux = donnees['hopitaux'].get('hopitaux', [])
                        all_hopitaux.extend(hopitaux)
                    
                    if 'statistiques_generales' in donnees:
                        all_statistiques.append({
                            'source': pdf_path.name,
                            'stats': donnees['statistiques_generales']
                        })
                    
                    # Sauvegarder résultat individuel
                    output_file = self.output_dir / 'metadata' / f"{pdf_path.stem}_extracted.json"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
            
            except Exception as e:
                logging.error(f"Erreur traitement {pdf_path.name}: {e}")
        
        # Sauvegarder données agrégées
        self._save_aggregated_data(all_hopitaux, all_statistiques, all_results)
        
        logging.info("\n" + "="*60)
        logging.info("EXTRACTION TERMINÉE!")
        logging.info("="*60)
        logging.info(f"PDFs traités: {len(all_results)}/{len(pdf_files)}")
        logging.info(f"Hôpitaux extraits: {len(all_hopitaux)}")
        logging.info(f"Documents statistiques: {len(all_statistiques)}")
        logging.info(f"\nRésultats dans: {self.output_dir}/")
        logging.info("="*60)
    
    def _save_aggregated_data(self, hopitaux, statistiques, all_results):
        """Sauvegarde les données agrégées"""
        
        # 1. Liste des hôpitaux
        if hopitaux:
            df_hopitaux = pd.DataFrame(hopitaux)
            df_hopitaux.to_csv(
                self.output_dir / 'hopitaux' / 'liste_hopitaux_extraite.csv',
                index=False, encoding='utf-8-sig'
            )
            df_hopitaux.to_excel(
                self.output_dir / 'hopitaux' / 'liste_hopitaux_extraite.xlsx',
                index=False, engine='openpyxl'
            )
            logging.info(f"✓ Liste hôpitaux sauvegardée: {len(hopitaux)} entrées")
        
        # 2. Statistiques
        if statistiques:
            with open(self.output_dir / 'statistiques' / 'statistiques_sante.json', 'w', encoding='utf-8') as f:
                json.dump(statistiques, f, ensure_ascii=False, indent=2)
            logging.info(f"✓ Statistiques sauvegardées")
        
        # 3. Rapport global
        rapport = {
            'date_extraction': datetime.now().isoformat(),
            'total_pdfs': len(all_results),
            'total_hopitaux': len(hopitaux),
            'total_stats': len(statistiques),
            'fichiers_traites': [r['fichier'] for r in all_results]
        }
        
        with open(self.output_dir / 'rapport_extraction.json', 'w', encoding='utf-8') as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)


def main():
    """Point d'entrée principal"""
    
    print("""
╔══════════════════════════════════════════════════════════╗
║   EXTRACTEUR DE DONNÉES HOSPITALIÈRES AVEC LLM          ║
╚══════════════════════════════════════════════════════════╝

Ce script utilise un LLM pour extraire intelligemment:
- Liste des hôpitaux (nom, ville, type, capacité)
- Statistiques sur les médecins
- Indicateurs de santé publique
- Données d'infrastructure sanitaire

CHOIX DU LLM:
1. Groq API (GRATUIT, RAPIDE) - Recommandé ✓
2. Ollama (LOCAL, GRATUIT)
    """)
    
    # Choix du provider
    choice = input("Choisissez (1 ou 2): ").strip()
    
    if choice == '1':
        provider = 'groq'
        api_key = input("\nClé API Groq (ou appuyez sur Entrée si dans .env): ").strip() or None
    else:
        provider = 'ollama'
        api_key = None
        print("\n⚠️  Assurez-vous qu'Ollama est lancé: ollama serve")
        print("   Et qu'un modèle est téléchargé: ollama pull llama3.2\n")
        input("Appuyez sur Entrée pour continuer...")
    
    # Créer extracteur
    extractor = HospitalDataExtractor(
        input_dir='ministere_sante',
        output_dir='donnees_extraites',
        llm_provider=provider,
        groq_api_key=api_key
    )
    
    # Lancer extraction
    extractor.process_all_pdfs()


if __name__ == "__main__":
    main()