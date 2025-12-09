# ==================== 3_enrichissement_llm.py ====================
"""
3_enrichissement_llm.py - VERSION OPENROUTER
Enrichit les données avec:
- OpenRouter API pour accès multi-modèles
- Gestion intelligente du rate limiting
- Fallback automatique entre modèles
- Traitement par batch optimisé
"""
import pandas as pd
import os
import json
import logging
import random
import shutil
import time
import requests
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class Enricher:
    def __init__(self):
        self.processed_dir = "data/processed"
        self.enriched_dir = "data/enriched"
        self.cache_dir = "data/cache"
        os.makedirs(self.enriched_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # ==============================
        # Configuration OpenRouter API
        # ==============================
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY")
        self.openrouter_url = "https://openrouter.ai/api/v1/chat/completions"
        
        # Modèles disponibles (du moins cher au plus cher)
        self.models = [
            "meta-llama/llama-3.1-8b-instruct",  # Gratuit, rapide
            "google/gemini-flash-1.5",                 # Bon rapport qualité/prix
            "meta-llama/llama-3.3-70b-instruct",      # Puissant
            "anthropic/claude-3.5-sonnet",            # Excellent mais plus cher
        ]
        
        self.current_model_index = 0
        self.current_model = self.models[self.current_model_index]
        
        # Vérifier si API disponible
        self.use_api = bool(self.openrouter_key)
        
        if self.use_api:
            logger.info(f"✅ OpenRouter configuré - Modèle: {self.current_model}")
        else:
            logger.warning("⚠️ Pas de clé OPENROUTER_API_KEY - Mode simulation")
        
        # OPTIMIZED: Configuration des requêtes
        self.batch_size = 10
        self.delay_between_requests = 1.5
        self.last_request_time = 0
        self.retry_delay = 30
        self.max_retries = 2
        self.quick_retry_delay = 5
        
        # Cache
        self.cache_file = f"{self.cache_dir}/enrichment_cache.json"
        self.cache = self._load_cache()
        
        # Cache web
        self.web_cache_file = f"{self.cache_dir}/web_search_cache.json"
        self.web_cache = self._load_web_cache()
        
        # Checkpoint
        self.checkpoint_file = f"{self.cache_dir}/checkpoint.json"
        self.checkpoint = self._load_checkpoint()

        # Chargement des médicaments
        self.medications = pd.DataFrame()
        med_path = f"{self.processed_dir}/medications.csv"
        if os.path.exists(med_path):
            self.medications = pd.read_csv(med_path)

    def _switch_model(self):
        """Passe au modèle suivant en cas d'échec"""
        self.current_model_index = (self.current_model_index + 1) % len(self.models)
        self.current_model = self.models[self.current_model_index]
        logger.info(f"🔄 Changement de modèle vers: {self.current_model}")

    def _call_llm_json(self, prompt, temperature=0.2, is_batch=False):
        """
        Appelle OpenRouter avec fallback automatique entre modèles
        """
        if not self.use_api:
            return None

        last_error = None
        models_tried = 0
        
        # Essayer tous les modèles disponibles
        while models_tried < len(self.models):
            try:
                self._wait_for_rate_limit()
                
                headers = {
                    "Authorization": f"Bearer {self.openrouter_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/yourusername/hospital-db",
                    "X-Title": "Hospital Database Enrichment"
                }
                
                payload = {
                    "model": self.current_model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "Tu es un assistant pour enrichir une base de données d'hôpitaux marocains. Tu dois TOUJOURS répondre avec un JSON STRICTEMENT VALIDE, sans texte avant ou après."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": temperature,
                    "response_format": {"type": "json_object"}
                }
                
                response = requests.post(
                    self.openrouter_url,
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                # Succès
                if response.status_code == 200:
                    data = response.json()
                    content = data['choices'][0]['message']['content']
                    return content
                
                # Rate limit (429)
                elif response.status_code == 429:
                    logger.debug(f"⏳ Rate limit sur {self.current_model}, attente {self.quick_retry_delay}s")
                    time.sleep(self.quick_retry_delay)
                    continue
                
                # Erreur spécifique au modèle (trop cher, indisponible, etc.)
                elif response.status_code in [400, 402, 503]:
                    error_data = response.json()
                    error_msg = error_data.get('error', {}).get('message', str(response.status_code))
                    logger.warning(f"⚠️ Erreur {self.current_model}: {error_msg}")
                    
                    # Passer au modèle suivant
                    self._switch_model()
                    models_tried += 1
                    continue
                
                # Autre erreur
                else:
                    logger.warning(f"⚠️ Erreur HTTP {response.status_code}: {response.text[:200]}")
                    time.sleep(self.quick_retry_delay)
                    continue
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Timeout sur {self.current_model}")
                time.sleep(self.quick_retry_delay)
                continue
                
            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"❌ Erreur réseau: {str(e)[:100]}")
                time.sleep(self.quick_retry_delay)
                continue
                
            except Exception as e:
                last_error = e
                logger.error(f"❌ Erreur inattendue: {str(e)[:100]}")
                break

        # Si tous les modèles ont échoué
        if is_batch:
            logger.warning("⚠️ Tous les modèles ont échoué pour le batch, utilisation simulation")
        
        return None

    def _load_cache(self):
        """Charge le cache des enrichissements déjà effectués"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_cache(self):
        """Sauvegarde le cache"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde cache: {e}")

    def _load_web_cache(self):
        """Charge le cache des recherches web"""
        if os.path.exists(self.web_cache_file):
            try:
                with open(self.web_cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_web_cache(self):
        """Sauvegarde le cache des recherches web"""
        try:
            with open(self.web_cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.web_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde web cache: {e}")

    def _load_checkpoint(self):
        """Charge le dernier checkpoint"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    cp = json.load(f)
                    logger.info(f"📍 Checkpoint trouvé: index {cp.get('last_processed_index', -1)}, date {cp.get('timestamp', 'N/A')}")
                    return cp
            except:
                return {'last_processed_index': -1}
        return {'last_processed_index': -1}

    def _save_checkpoint(self, index):
        """Sauvegarde un checkpoint"""
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_processed_index': index, 
                    'timestamp': str(datetime.now())
                }, f)
        except Exception as e:
            logger.error(f"Erreur sauvegarde checkpoint: {e}")

    def _wait_for_rate_limit(self):
        """Gère le rate limiting"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.delay_between_requests:
            sleep_time = self.delay_between_requests - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def search_hospital_info_with_llm(self, hospital_name, city, current_data):
        """
        Utilise le LLM pour trouver les informations manquantes.
        OPTIMIZED: Skip if most data already present
        """
        cache_key = f"web_{hospital_name}_{city}"

        # Vérifier le cache web d'abord
        if cache_key in self.web_cache:
            return self.web_cache[cache_key]

        if not self.use_api:
            return None

        # OPTIMIZED: Count missing fields
        missing_fields = []
        if pd.isna(current_data.get("address")) or not current_data.get("address"):
            missing_fields.append("address")
        if pd.isna(current_data.get("phone")) or not current_data.get("phone"):
            missing_fields.append("phone")
        if pd.isna(current_data.get("email")) or not current_data.get("email"):
            missing_fields.append("email")
        if pd.isna(current_data.get("website")) or not current_data.get("website"):
            missing_fields.append("website")
        if pd.isna(current_data.get("beds")) or current_data.get("beds", 0) == 0:
            missing_fields.append("beds")

        # Skip if less than 2 fields missing
        if len(missing_fields) < 2:
            return None

        prompt = f"""
Recherche des informations sur cet hôpital marocain:
Nom: {hospital_name}
Ville: {city}

Informations manquantes à trouver: {', '.join(missing_fields)}

Retourne un JSON avec cette structure:
{{
    "address": "adresse complète si trouvée, sinon null",
    "phone": "numéro de téléphone si trouvé, sinon null",
    "email": "email si trouvé, sinon null",
    "website": "URL du site web si trouvé, sinon null",
    "beds": nombre de lits si trouvé (entier), sinon null,
    "source_quality": "high/medium/low"
}}

IMPORTANT: Vérifie que les informations correspondent bien à cet hôpital spécifique.
"""

        content = self._call_llm_json(prompt, temperature=0.2, is_batch=False)
        if not content:
            return None

        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Réponse LLM non JSON pour {hospital_name}")
            return None

        result = self._validate_hospital_info(result)

        # Mettre en cache
        self.web_cache[cache_key] = result
        if len(self.web_cache) % 20 == 0:  # Save every 20 entries
            self._save_web_cache()

        logger.info(f"✓ Info trouvées pour {hospital_name}: {result.get('source_quality', 'unknown')} quality")
        return result

    def _validate_hospital_info(self, info):
        """Valide et nettoie les informations trouvées"""
        if not info:
            return None
        
        # Validation du téléphone
        if info.get('phone'):
            phone = str(info['phone'])
            if not (phone.startswith('+212') or phone.startswith('0')):
                info['phone'] = None
        
        # Validation de l'email
        if info.get('email'):
            email = str(info['email'])
            if '@' not in email or '.' not in email:
                info['email'] = None
        
        # Validation du website
        if info.get('website'):
            website = str(info['website'])
            if not (website.startswith('http://') or website.startswith('https://')):
                if website.startswith('www.'):
                    info['website'] = f"https://{website}"
                else:
                    info['website'] = None
        
        # Validation des lits
        if info.get('beds'):
            try:
                beds = int(info['beds'])
                if beds < 0 or beds > 10000:
                    info['beds'] = None
            except:
                info['beds'] = None
        
        return info

    def load_reference_ids(self):
        """Charge les IDs valides"""
        self.valid_services = []
        self.valid_equipment = []
        
        path_srv = f"{self.processed_dir}/services.csv"
        if os.path.exists(path_srv):
            df = pd.read_csv(path_srv)
            self.valid_services = df['id'].tolist()
            
        path_eq = f"{self.processed_dir}/equipment.csv"
        if os.path.exists(path_eq):
            df = pd.read_csv(path_eq)
            self.valid_equipment = df['id'].tolist()

    def get_simulated_data(self, hospital_type):
        """Génère des données cohérentes si pas d'API LLM"""
        htype = str(hospital_type).lower()
        
        if 'chu' in htype or 'universitaire' in htype or 'régional' in htype:
            nb_services = random.randint(10, 20)
            nb_equip = random.randint(15, 50)
            beds = random.randint(300, 800)
        elif 'clinique' in htype or 'polyclinique' in htype:
            nb_services = random.randint(5, 12)
            nb_equip = random.randint(5, 20)
            beds = random.randint(30, 150)
        else:
            nb_services = random.randint(1, 5)
            nb_equip = random.randint(1, 5)
            beds = random.randint(10, 60)
            
        s_ids = random.sample(self.valid_services, min(nb_services, len(self.valid_services))) if self.valid_services else []
        e_ids = random.sample(self.valid_equipment, min(nb_equip, len(self.valid_equipment))) if self.valid_equipment else []
        
        return s_ids, e_ids, beds

    def generate_medication_stock(self, hospital_id):
        """Génère un stock de médicaments simulé"""
        if self.medications.empty:
            return []
            
        nb_meds = random.randint(10, min(50, len(self.medications)))
        selected_meds = self.medications.sample(n=nb_meds)
        
        stock_data = []
        for _, med in selected_meds.iterrows():
            stock_data.append({
                'hospital_id': hospital_id,
                'medication_id': med['id'],
                'stock_quantity': random.randint(10, 1000)
            })
        return stock_data

    def infer_details_batch_llm(self, hospitals_batch):
        """Traite plusieurs hôpitaux en batch."""
        if not self.use_api:
            return None

        hospital_list = []
        for h in hospitals_batch:
            hospital_list.append(f"- ID {h['id']}: '{h['name']}' (Type: {h['type']})")

        prompt = f"""
Analyse ces hôpitaux marocains et pour chacun, estime le nombre de services, équipements et lits.

Hôpitaux:
{chr(10).join(hospital_list[:self.batch_size])}

Retourne un JSON:
{{
    "hospitals": [
        {{"id": 1, "service_count": 15, "equipment_count": 25, "beds": 400}}
    ]
}}

Base tes estimations sur:
- CHU/Universitaire/Régional: 10-20 services, 15-50 équipements, 300-800 lits
- Clinique/Polyclinique: 5-12 services, 5-20 équipements, 30-150 lits
- Centre/Dispensaire/Local: 1-5 services, 1-5 équipements, 10-60 lits
"""

        content = self._call_llm_json(prompt, temperature=0.3, is_batch=True)
        if not content:
            return None

        try:
            data = json.loads(content)
            return data.get("hospitals", [])
        except json.JSONDecodeError:
            logger.warning(f"Réponse LLM batch non JSON")
            return None

    def process_hospital(self, hospital_row, cache_key):
        """Traite un seul hôpital avec cache"""
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            return cached['s_ids'], cached['e_ids'], cached['beds']
        
        s_ids, e_ids, beds = self.get_simulated_data(hospital_row['type'])
        
        self.cache[cache_key] = {
            's_ids': s_ids,
            'e_ids': e_ids,
            'beds': beds
        }
        
        return s_ids, e_ids, beds

    def enrich_hospital_with_web_search(self, row):
        """Enrichit un hôpital avec les informations web"""
        current_data = {
            'address': row.get('address'),
            'phone': row.get('phone'),
            'email': row.get('email'),
            'website': row.get('website'),
            'beds': row.get('beds')
        }
        
        web_info = self.search_hospital_info_with_llm(
            row['name'],
            row.get('city', ''),
            current_data
        )
        
        if web_info:
            updates = {}
            
            if web_info.get('address') and (pd.isna(current_data['address']) or not current_data['address']):
                updates['address'] = web_info['address']
            
            if web_info.get('phone') and (pd.isna(current_data['phone']) or not current_data['phone']):
                updates['phone'] = web_info['phone']
            
            if web_info.get('email') and (pd.isna(current_data['email']) or not current_data['email']):
                updates['email'] = web_info['email']
            
            if web_info.get('website') and (pd.isna(current_data['website']) or not current_data['website']):
                updates['website'] = web_info['website']
            
            if web_info.get('beds') and (pd.isna(current_data['beds']) or current_data['beds'] == 0):
                updates['beds'] = web_info['beds']
            
            return updates
        
        return {}

    def save_progress(self, df_hospitals, rel_services, rel_equipment, rel_medications):
        """Sauvegarde tous les progrès"""
        try:
            hospitals_path = f"{self.enriched_dir}/hospitals.csv"
            df_hospitals.to_csv(hospitals_path, index=False, encoding='utf-8-sig')
            
            if rel_services:
                pd.DataFrame(rel_services).to_csv(
                    f"{self.enriched_dir}/hospital_services.csv", 
                    index=False, encoding='utf-8-sig'
                )
            
            if rel_equipment:
                pd.DataFrame(rel_equipment).to_csv(
                    f"{self.enriched_dir}/hospital_equipment.csv", 
                    index=False, encoding='utf-8-sig'
                )
            
            if rel_medications:
                pd.DataFrame(rel_medications).to_csv(
                    f"{self.enriched_dir}/hospital_medications.csv",
                    index=False, encoding='utf-8-sig'
                )
            
            self._save_cache()
            self._save_web_cache()
            
            return True
        except Exception as e:
            logger.error(f"Erreur sauvegarde progrès: {e}")
            return False

    def run(self):
        logger.info("=== ENRICHISSEMENT AVEC OPENROUTER ===")
        
        # 1. Copie des fichiers de base
        files_to_copy = ['hospitals.csv', 'places.csv', 'services.csv', 
                         'equipment.csv', 'medications.csv', 'suppliers.csv']
        
        supplier_med_src = f"{self.processed_dir}/supplier_medications.csv"
        if os.path.exists(supplier_med_src):
            files_to_copy.append('supplier_medications.csv')
        
        for f in files_to_copy:
            src = f"{self.processed_dir}/{f}"
            dst = f"{self.enriched_dir}/{f}"
            if os.path.exists(src):
                shutil.copy(src, dst)
            else:
                logger.warning(f"Fichier source manquant: {src}")

        # 2. Chargement des références
        self.load_reference_ids()
        if not self.valid_services or not self.valid_equipment:
            logger.error("Impossible de charger les services ou équipements.")
            return

        hospitals_path = f"{self.enriched_dir}/hospitals.csv"
        if not os.path.exists(hospitals_path):
            logger.error("hospitals.csv introuvable.")
            return

        df_hospitals = pd.read_csv(hospitals_path)
        total_hospitals = len(df_hospitals)
        
        start_index = self.checkpoint['last_processed_index'] + 1
        if start_index > 0:
            logger.info(f"🔄 Reprise depuis l'hôpital #{start_index}/{total_hospitals}")
        
        rel_services = []
        rel_equipment = []
        rel_medications = []
        
        stats = {
            'web_searches': 0,
            'info_found': 0,
            'addresses_found': 0,
            'phones_found': 0,
            'emails_found': 0,
            'websites_found': 0,
            'beds_found': 0
        }
        
        if self.use_api:
            logger.info(f"Mode: OpenRouter API")
            logger.info(f"  - Modèle actuel: {self.current_model}")
            logger.info(f"  - Modèles disponibles: {len(self.models)}")
            logger.info(f"  - Batch size: {self.batch_size}")
            logger.info(f"  - Délai entre requêtes: {self.delay_between_requests}s")
        else:
            logger.info("Mode: Simulation (pas de clé API)")
        
        processed = start_index
        start_time = time.time()
        
        try:
            for idx in range(start_index, total_hospitals, self.batch_size if self.use_api else 1):
                batch_end = min(idx + self.batch_size, total_hospitals)
                batch = df_hospitals.iloc[idx:batch_end]
                
                # Traitement par batch
                batch_results = None
                if self.use_api and len(batch) > 1:
                    batch_hospitals = [
                        {'id': row['id'], 'name': row['name'], 'type': row['type']}
                        for _, row in batch.iterrows()
                    ]
                    batch_results = self.infer_details_batch_llm(batch_hospitals)
                
                # Traiter chaque hôpital
                for batch_idx, (df_idx, row) in enumerate(batch.iterrows()):
                    h_id = row['id']
                    h_name = row['name']
                    h_type = row['type']
                    cache_key = f"{h_id}_{h_name}"
                    
                    # 1. Recherche web (optimisé - skip si peu de champs manquants)
                    web_updates = self.enrich_hospital_with_web_search(row)
                    if web_updates:
                        stats['web_searches'] += 1
                        stats['info_found'] += 1
                        
                        for field, value in web_updates.items():
                            df_hospitals.at[df_idx, field] = value
                            stats[f'{field}s_found'] = stats.get(f'{field}s_found', 0) + 1
                    
                    # 2. Services et équipements
                    if batch_results and batch_idx < len(batch_results):
                        br = batch_results[batch_idx]
                        s_count = br.get('service_count', 5)
                        e_count = br.get('equipment_count', 5)
                        beds = br.get('beds', 50)
                        
                        s_ids = random.sample(self.valid_services, min(s_count, len(self.valid_services)))
                        e_ids = random.sample(self.valid_equipment, min(e_count, len(self.valid_equipment)))
                        
                        self.cache[cache_key] = {'s_ids': s_ids, 'e_ids': e_ids, 'beds': beds}
                    else:
                        s_ids, e_ids, beds = self.process_hospital(row, cache_key)
                    
                    if pd.isna(df_hospitals.at[df_idx, 'beds']) or df_hospitals.at[df_idx, 'beds'] == 0:
                        df_hospitals.at[df_idx, 'beds'] = beds
                    
                    # Relations
                    for sid in s_ids:
                        rel_services.append({'hospital_id': h_id, 'service_id': sid})
                        
                    for eid in e_ids:
                        qty = random.randint(1, 5)
                        rel_equipment.append({'hospital_id': h_id, 'equipment_id': eid, 'quantity': qty})
                    
                    med_stocks = self.generate_medication_stock(h_id)
                    rel_medications.extend(med_stocks)

                    processed += 1
                    
                    # Sauvegarde tous les 20 hôpitaux
                    if processed % 20 == 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed * 60
                        logger.info(f"💾 Sauvegarde à {processed}/{total_hospitals} ({rate:.1f} hôpitaux/min)")
                        self.save_progress(df_hospitals, rel_services, rel_equipment, rel_medications)
                        self._save_checkpoint(df_idx)
                    
                    # Progress tous les 100
                    if processed % 100 == 0:
                        pct = processed * 100 // total_hospitals
                        elapsed = time.time() - start_time
                        rate = processed / elapsed * 60
                        eta = (total_hospitals - processed) / rate if rate > 0 else 0
                        logger.info(f"📊 {processed}/{total_hospitals} ({pct}%) - {rate:.1f}/min - ETA: {eta:.0f}min - Web: {stats['web_searches']}")
        
        except KeyboardInterrupt:
            logger.warning("\n⚠️ Interruption détectée! Sauvegarde de l'avancement...")
            self.save_progress(df_hospitals, rel_services, rel_equipment, rel_medications)
            self._save_checkpoint(processed - 1)
            logger.info(f"✓ Avancement sauvegardé à l'index {processed - 1}")
            logger.info("ℹ️ Relancez le script pour continuer depuis ce point")
            return
        
        except Exception as e:
            logger.error(f"❌ Erreur critique: {e}")
            self.save_progress(df_hospitals, rel_services, rel_equipment, rel_medications)
            self._save_checkpoint(processed - 1)
            raise
        
        # Sauvegarde finale
        self.save_progress(df_hospitals, rel_services, rel_equipment, rel_medications)
        
        # Nettoyage checkpoint
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
        
        # Stats finales
        elapsed = time.time() - start_time
        rate = processed / elapsed * 60 if elapsed > 0 else 0

        logger.info("============================================")
        logger.info("✅ Enrichissement terminé")
        logger.info(f"   Hôpitaux traités : {processed}/{total_hospitals}")
        logger.info(f"   Durée totale      : {elapsed/60:.1f} min")
        logger.info(f"   Vitesse moyenne   : {rate:.1f} hôpitaux/min")

        if self.use_api:
            logger.info("   --- Statistiques Web ---")
            logger.info(f"   Recherches web LLM : {stats.get('web_searches', 0)}")
            logger.info(f"   Hôpitaux enrichis  : {stats.get('info_found', 0)}")
            logger.info(f"   Adresses trouvées  : {stats.get('addresses_found', 0)}")
            logger.info(f"   Téléphones trouvés : {stats.get('phones_found', 0)}")
            logger.info(f"   Emails trouvés     : {stats.get('emails_found', 0)}")
            logger.info(f"   Sites web trouvés  : {stats.get('websites_found', 0)}")
            logger.info(f"   Nombres de lits MAJ: {stats.get('beds_found', 0)}")

        logger.info("--------------------------------------------")
        logger.info(f"📁 Fichiers enrichis dans : {self.enriched_dir}")
        logger.info("   - hospitals.csv")
        logger.info("   - hospital_services.csv")
        logger.info("   - hospital_equipment.csv")
        logger.info("   - hospital_medications.csv")
        logger.info("============================================")


if __name__ == "__main__":
    try:
        enricher = Enricher()
        enricher.run()
    except Exception as e:
        logger.error(f"❌ Erreur inattendue au niveau global: {e}")
        raise
