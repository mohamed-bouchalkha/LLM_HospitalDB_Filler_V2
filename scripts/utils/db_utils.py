# scripts/utils/db_utils.py
import os
from sqlalchemy import create_engine


def get_mysql_engine():
    """
    Returns a SQLAlchemy engine for your MySQL DB.

    Set these environment variables in PowerShell or system env:
      $env:MYSQL_USER="root"
      $env:MYSQL_PASSWORD="your_password"
      $env:MYSQL_HOST="127.0.0.1"
      $env:MYSQL_PORT="3306"
      $env:MYSQL_DB="hospital_db"

    Uses utf8mb4 to match your schema.
    """
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3306")
    db = os.getenv("MYSQL_DB", "hospital_db")

    # mysql+pymysql is a standard combo for SQLAlchemy + MySQL
    # charset=utf8mb4 recommended for full Unicode. :contentReference[oaicite:5]{index=5}
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"

    engine = create_engine(url, echo=False, pool_pre_ping=True)
    return engine
