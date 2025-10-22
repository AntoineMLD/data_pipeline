# File: src/import_to_postgres.py
"""
Migration DuckDB (src/data/taxi_data.duckdb) -> PostgreSQL via pandas.to_sql().
- Lecture DuckDB en chunks (500k lignes par défaut) pour limiter la RAM.
- Normalisation des noms de colonnes en snake_case avant insertion.
- Création des tables cibles si absentes (DDL minimal).
"""

from __future__ import annotations

import os
import math
import re
from pathlib import Path
from typing import Iterator, Optional

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Fichier source DuckDB (fixe)
DUCKDB_PATH = Path("src/data/taxi_data.duckdb")

# ------------------------------
# Connexion PostgreSQL (env vars)
# ------------------------------
def build_postgres_url_from_env() -> str:
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "nyc_taxi")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

# ------------------------------
# Normalisation des noms de colonnes
# ------------------------------
def snake_case(name: str) -> str:
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [snake_case(c) for c in df.columns]
    return df

# --------------------------------------------
# Lecture DuckDB par chunks (DEFAULT: 500k)
# --------------------------------------------
def iter_duckdb_table_chunks(
    db_path: Path,
    table_name: str,
    chunksize: int = 500_000,  # augmenté à 500k
) -> Iterator[pd.DataFrame]:
    con = duckdb.connect(str(db_path))
    try:
        total = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if total == 0:
            return
        pages = math.ceil(total / chunksize)
        for i in range(pages):
            offset = i * chunksize
            df = con.execute(
                f"SELECT * FROM {table_name} LIMIT {chunksize} OFFSET {offset}"
            ).df()
            yield df
    finally:
        con.close()

# ------------------------------------
# DDL cibles PostgreSQL (création si absent)
# ------------------------------------
def ensure_tables(engine: Engine) -> None:
    create_trips = """
    CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
        vendorid BIGINT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE PRECISION,
        trip_distance DOUBLE PRECISION,
        ratecodeid DOUBLE PRECISION,
        store_and_fwd_flag VARCHAR,
        pulocationid BIGINT,
        dolocationid BIGINT,
        payment_type BIGINT,
        fare_amount DOUBLE PRECISION,
        extra DOUBLE PRECISION,
        mta_tax DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        improvement_surcharge DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        congestion_surcharge DOUBLE PRECISION,
        airport_fee DOUBLE PRECISION,
        cbd_congestion_fee DOUBLE PRECISION
    );
    """
    create_log = """
    CREATE TABLE IF NOT EXISTS import_log (
        file_name VARCHAR,
        import_date TIMESTAMP,
        rows_imported BIGINT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_trips))
        conn.execute(text(create_log))

# -------------------------------
# Importeur PostgreSQL
# -------------------------------
class PostgresImporter:
    def __init__(self, postgres_url: Optional[str] = None) -> None:
        if not DUCKDB_PATH.exists():
            raise FileNotFoundError(f"Base DuckDB introuvable : {DUCKDB_PATH.resolve()}")
        self.postgres_url = postgres_url or build_postgres_url_from_env()
        self.engine: Engine = create_engine(self.postgres_url, pool_pre_ping=True, future=True)
        ensure_tables(self.engine)

    def _append_df(self, df: pd.DataFrame, table: str) -> int:
        if df.empty:
            return 0
        df = normalize_columns(df)
        df.to_sql(
            name=table,
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,  # chunk d'INSERT côté pandas (ne pas confondre avec lecture DuckDB)
        )
        return len(df)

    def import_yellow_taxi_trips(self, chunksize: int = 500_000) -> int:  # 500k par défaut
        total = 0
        for df in iter_duckdb_table_chunks(DUCKDB_PATH, "yellow_taxi_trips", chunksize=chunksize):
            total += self._append_df(df, "yellow_taxi_trips")
            print(f"[trips] +{len(df):,} lignes (total: {total:,})")
        return total

    def import_import_log(self, chunksize: int = 500_000) -> int:  # 500k par défaut
        total = 0
        for df in iter_duckdb_table_chunks(DUCKDB_PATH, "import_log", chunksize=chunksize):
            total += self._append_df(df, "import_log")
            print(f"[log] +{len(df):,} lignes (total: {total:,})")
        return total

    def migrate_all(self, chunksize: int = 500_000) -> None:  # 500k par défaut
        print(f"[start] Migration DuckDB → Postgres")
        print(f"        source : {DUCKDB_PATH.resolve()}")
        print(f"        target : {self.postgres_url}")
        trips = self.import_yellow_taxi_trips(chunksize=chunksize)
        logs = self.import_import_log(chunksize=chunksize)
        print("\n[summary]")
        print(f"  ✓ yellow_taxi_trips : {trips:,} lignes insérées")
        print(f"  ✓ import_log        : {logs:,} lignes insérées")

if __name__ == "__main__":
    importer = PostgresImporter()
    importer.migrate_all(chunksize=500_000)
