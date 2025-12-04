import os
import json
import time
import pandas as pd
import google.generativeai as genai
from groq import Groq
import logging

# CONFIGURATION
INPUT_CORPUS = "donnees_extraites/structured/raw_corpus_global.txt"
OUTPUT_JSON = "donnees_extraites/hopitaux/db_hopitaux_final.json"
OUTPUT_CSV = "donnees_extraites/hopitaux/db_hopitaux_final.csv"
CHECKPOINT_FILE = "checkpoint.txt"
CHUNK_SIZE = 25000 
PROVIDER = "gemini" 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_llm_response_safe(prompt, retries=5):
    """Robust API call with retry logic for 429/500 errors."""
    if PROVIDER == "gemini":
        genai.configure(api_key = os.getenv("GOOGLE_API_KEY") or "VOTRE_CLE_EN_DUR_ICI")
        # Use Flash-Lite for higher limits or Flash 2.0
        model = genai.GenerativeModel('models/gemini-2.5-flash-lite') 
        
        wait_time = 10 
        for attempt in range(retries):
            try:
                resp = model.generate_content(prompt)
                return resp.text
            except Exception as e:
                if "429" in str(e) or "quota" in str(e).lower():
                    logging.warning(f"Quota exceeded. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    wait_time *= 2 
                else:
                    logging.error(f"API Error: {e}")
                    return "{}"
        return "{}"

    elif PROVIDER == "groq":
        # Add Groq implementation if needed
        pass

def load_checkpoint():
    """Reads the last processed chunk index from file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return int(f.read().strip())
        except ValueError:
            return 0
    return 0

def save_checkpoint(index):
    """Saves the current chunk index."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(index))

def append_to_database(new_hospitals):
    """
    Safely loads the existing JSON, appends new data, and saves it back.
    This ensures we don't overwrite previous work.
    """
    existing_data = []
    
    # 1. Load existing data if file exists
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except json.JSONDecodeError:
            logging.warning("Existing JSON was corrupt. Starting fresh.")
            existing_data = []
    
    # 2. Append new data
    existing_data.extend(new_hospitals)
    
    # 3. Write back to file
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)
        
    return len(existing_data)

def build_prompt(chunk):
    return f"""
    EXTRACT HOSPITALS from this text.
    Return JSON only: {{ "hospitals": [ {{ "nom": "...", "ville": "...", "lits": 0 }} ] }}
    Text: {chunk[:10000]}... (truncated for brevity)
    """

def main():
    if not os.path.exists(INPUT_CORPUS):
        print("Error: Corpus file not found.")
        return

    with open(INPUT_CORPUS, "r", encoding="utf-8") as f:
        text = f.read()

    chunks = [text[i:i+CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]
    
    # RESUME LOGIC
    start_index = load_checkpoint()
    print(f"--- RESUMING FROM CHUNK {start_index + 1}/{len(chunks)} ---")

    for i, chunk in enumerate(chunks):
        # Skip chunks we already processed
        if i < start_index:
            continue

        print(f"Processing Chunk {i+1}/{len(chunks)}...")
        
        prompt = build_prompt(chunk)
        response_json = get_llm_response_safe(prompt)
        
        try:
            cleaned_json = response_json.replace("```json", "").replace("```", "").strip()
            if cleaned_json:
                data = json.loads(cleaned_json)
                new_hospitals = data.get("hospitals", [])
                
                if new_hospitals:
                    total_count = append_to_database(new_hospitals)
                    print(f"   -> Added {len(new_hospitals)} hospitals. Total in DB: {total_count}")
                else:
                    print("   -> No hospitals found in this chunk.")
        except json.JSONDecodeError:
            logging.warning(f"Failed to parse JSON for chunk {i+1}")

        # SAVE PROGRESS
        save_checkpoint(i + 1)
        
        # Rate limit pause
        time.sleep(5)

    # Final CSV Conversion
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            all_data = json.load(f)
        if all_data:
            df = pd.json_normalize(all_data)
            df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
            print(f"Done! Final CSV saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()