"""
Utility to import processed CSV into MySQL (XAMPP/phpMyAdmin).
"""

import argparse
import os
import pandas as pd
import yaml
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_mysql_engine(cfg: dict):
    required = ["host", "user", "password", "database"]
    if not all(k in cfg for k in required):
        raise ValueError(f"MySQL config missing required keys: {required}")

    host = cfg.get("host", "localhost")
    port = cfg.get("port", 3306)
    user = cfg["user"]
    password = cfg.get("password", "")
    database = cfg["database"]

    uri = (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{database}?charset=utf8mb4"
    )
    return create_engine(uri, pool_pre_ping=True)


def main():
    parser = argparse.ArgumentParser(description="Import hospitals CSV into MySQL")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config file with mysql block",
    )
    parser.add_argument(
        "--csv",
        default="data/processed/hospitals_comprehensive.csv",
        help="CSV file to import",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Destination table (defaults to config.mysql.table or hospitals_comprehensive)",
    )
    parser.add_argument(
        "--if-exists",
        default=None,
        choices=["fail", "replace", "append"],
        help="Behavior if table exists (overrides config.mysql.if_exists)",
    )

    args = parser.parse_args()

    config = load_config(args.config)
    mysql_cfg = config.get("mysql", {})
    if not mysql_cfg.get("enabled"):
        logger.warning("mysql.enabled is false in config; set it to true to use this importer.")

    table = args.table or mysql_cfg.get("table", "hospitals_comprehensive")
    if_exists = args.if_exists or mysql_cfg.get("if_exists", "replace")

    if not os.path.exists(args.csv):
        raise FileNotFoundError(f"CSV not found at {args.csv}")

    engine = build_mysql_engine(mysql_cfg)
    df = pd.read_csv(args.csv, encoding="utf-8")
    logger.info(f"Loaded {len(df)} rows from {args.csv}")

    df.to_sql(
        table,
        con=engine,
        if_exists=if_exists,
        index=False,
        chunksize=500,
        method="multi",
    )
    logger.info(f"Imported {len(df)} rows into MySQL table '{table}'")


if __name__ == "__main__":
    main()

