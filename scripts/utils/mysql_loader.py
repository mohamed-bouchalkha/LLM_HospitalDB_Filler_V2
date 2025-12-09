import pandas as pd
from sqlalchemy import create_engine, text
import yaml
import os

def load_relational_data():
    # 1. Setup Connection
    with open("config/config.yaml", "r") as f:
        cfg = yaml.safe_load(f)['mysql']
    
    uri = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    engine = create_engine(uri)

    # 2. Define the Loading Order (Critical for Foreign Keys)
    # Tuple format: (csv_filename, table_name)
    load_order = [
        ("places.csv", "places"),
        ("hospitals.csv", "hospitals"),
        ("services.csv", "services"),
        ("medications.csv", "medications"),
        ("education_programs.csv", "education_programs"),
        ("hospital_services.csv", "hospital_services"),
        ("hospital_medications.csv", "hospital_medications"),
        ("hospital_education.csv", "hospital_education")
    ]

    with engine.connect() as conn:
        # Optional: Disable FK checks temporarily for faster bulk loading
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        
        for filename, table_name in load_order:
            file_path = f"data/relational/{filename}"
            if not os.path.exists(file_path):
                print(f"Skipping {filename} (not found)")
                continue
                
            print(f"Loading {filename} into table '{table_name}'...")
            df = pd.read_csv(file_path)
            
            # Write to SQL
            # if_exists='append' is safer for preserving schema constraints
            # We clear the table first manually if we want a fresh start
            conn.execute(text(f"TRUNCATE TABLE {table_name};")) 
            
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"✓ Loaded {len(df)} rows.")

        # Re-enable FK checks
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        conn.commit()

if __name__ == "__main__":
    load_relational_data()