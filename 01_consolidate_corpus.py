import os
import pandas as pd
import pdfplumber
import logging
import warnings
from pathlib import Path

# --- 1. CONFIGURATION TO SILENCE WARNINGS ---
class PDFWarningFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        # Filter out specific pdfminer/pdfplumber color warnings
        if 'Cannot set gray' in msg or 'invalid float value' in msg:
            return False
        return True

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Apply the filter to the root logger and all handlers
root_logger = logging.getLogger()
pdf_filter = PDFWarningFilter()
root_logger.addFilter(pdf_filter)
for handler in root_logger.handlers:
    handler.addFilter(pdf_filter)

# Aggressively silence pdfminer/pdfplumber loggers
logging.getLogger('pdfminer').setLevel(logging.ERROR)
logging.getLogger('pdfplumber').setLevel(logging.ERROR)
warnings.filterwarnings('ignore', category=UserWarning)
# --------------------------------------------

# Directories where your scrapers saved data
SOURCE_DIRS = [
    "ministere_sante/sante_en_chiffres_pdf",
    "ministere_sante/hopitaux",
    "hcp_donnees/indicateurs_sociaux",
    "data_gov_ma_sante/datasets_xlsx"
]
OUTPUT_FILE = "donnees_extraites/structured/raw_corpus_global.txt"

def extract_text_from_pdf(path):
    text = ""
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                try:
                    extracted = page.extract_text()
                    if extracted:
                        text += extracted + "\n"
                except Exception:
                    # Skip pages that are completely broken
                    continue
    except Exception as e:
        # Log only real errors, not warnings
        logging.warning(f"Could not read PDF {path.name}: {e}")
    return text

def extract_text_from_excel(path):
    try:
        # Read all sheets, convert to string
        df_dict = pd.read_excel(path, sheet_name=None, dtype=str)
        text_accum = ""
        for sheet_name, df in df_dict.items():
            text_accum += f"\n--- Sheet: {sheet_name} ---\n"
            text_accum += df.to_string(index=False)
        return text_accum
    except Exception as e:
        logging.warning(f"Could not read Excel {path.name}: {e}")
        return ""

def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    full_corpus = ""
    
    print("Starting consolidation... (Warnings are now suppressed)")
    
    for folder in SOURCE_DIRS:
        path = Path(folder)
        if not path.exists():
            logging.warning(f"Folder not found: {folder}")
            continue
            
        logging.info(f"Processing folder: {folder}")
        for file_path in path.rglob("*"):
            # Check for PDF
            if file_path.suffix.lower() == ".pdf":
                logging.info(f"Extracting PDF: {file_path.name}")
                extracted = extract_text_from_pdf(file_path)
                if extracted:
                    full_corpus += f"\n\n=== SOURCE: {file_path.name} ===\n" + extracted
            
            # Check for Excel/CSV
            elif file_path.suffix.lower() in [".xlsx", ".xls", ".csv"]:
                logging.info(f"Extracting Table: {file_path.name}")
                extracted = extract_text_from_excel(file_path)
                if extracted:
                    full_corpus += f"\n\n=== SOURCE: {file_path.name} ===\n" + extracted

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(full_corpus)
    
    print("-" * 30)
    logging.info(f"Corpus generated: {OUTPUT_FILE}")
    logging.info(f"Total size: {len(full_corpus)} characters")

if __name__ == "__main__":
    main()