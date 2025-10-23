"""
Pipeline de nettoyage des données : charge depuis PostgreSQL, analyse, nettoie,
et sauvegarde dans MongoDB.

- Table source : yellow_taxi_trips (créée par import_to_postgres.py)
- Collection cible MongoDB : cleaned_trips

Usage (dans le conteneur) :
    python -m src.data_cleaner

Variables d'environnement requises (voir env.example.md) :
    POSTGRES_* et MONGO_*
"""
import os
from typing import Dict, Any, List, Iterator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pymongo import MongoClient


POSTGRES_TABLE = "yellow_taxi_trips"
MONGO_COLLECTION = "cleaned_trips"


class DataCleaner:
    """Charge les trajets NYC Taxi depuis PostgreSQL, analyse, nettoie,
    puis sauvegarde dans MongoDB.

    La classe est volontairement simple et explicite pour être lisible
    par un développeur débutant.
    """

    def __init__(self) -> None:
        """Initialise les connexions PostgreSQL et MongoDB.

        - Crée un engine SQLAlchemy à partir des variables d'environnement
        - Crée un client MongoDB à partir des variables d'environnement
        - Initialise la collection cible
        """
        self.pg_engine: Engine = self._get_postgres_engine()
        self.mongo_client: MongoClient = self._get_mongo_client()
        mongo_db_name = os.getenv("MONGO_DB", "nyc_taxi_clean")
        self.mongo_db = self.mongo_client[mongo_db_name]
        self.collection = self.mongo_db[MONGO_COLLECTION]

    # ------------------------------------------------------------------
    # Connexions
    # ------------------------------------------------------------------
    def _get_postgres_engine(self) -> Engine:
        """Crée un Engine SQLAlchemy pour PostgreSQL via les variables d'env.

        Variables attendues :
            POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
        """
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        db = os.getenv("POSTGRES_DB", "taxi_data")
        user = os.getenv("POSTGRES_USER", "pipeline_user")
        password = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
        url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        # server_side_cursors=True pour psycopg2 → streaming côté serveur (moins de RAM)
        return create_engine(url, pool_pre_ping=True, server_side_cursors=True)

    def _get_mongo_client(self) -> MongoClient:
        """Crée un client MongoDB via les variables d'environnement.

        Variables attendues :
            MONGO_USER, MONGO_PASSWORD, MONGO_HOST, MONGO_PORT
        """
        user = os.getenv("MONGO_USER", "admin")
        password = os.getenv("MONGO_PASSWORD", "admin")
        host = os.getenv("MONGO_HOST", "mongodb")
        port = int(os.getenv("MONGO_PORT", "27017"))
        # Format : mongodb://user:password@host:port/
        mongo_url = f"mongodb://{user}:{password}@{host}:{port}/"
        return MongoClient(mongo_url)

    # ------------------------------------------------------------------
    # Infos table
    # ------------------------------------------------------------------
    def get_total_rows(self) -> int:
        """Retourne le nombre total de lignes dans la table source."""
        with self.pg_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {POSTGRES_TABLE}"))
            value = result.scalar()
            return int(value or 0)

    # ------------------------------------------------------------------
    # Chargement (plein et streaming)
    # ------------------------------------------------------------------
    def load_data_from_postgres(self) -> pd.DataFrame:
        """Charge toutes les lignes depuis PostgreSQL dans un DataFrame pandas.

        Returns:
            DataFrame avec toutes les colonnes de yellow_taxi_trips.
        """
        query = text(f"SELECT * FROM {POSTGRES_TABLE}")
        with self.pg_engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def iter_data_from_postgres(self, chunksize: int = 100_000) -> Iterator[pd.DataFrame]:
        """Lit les données PostgreSQL en flux (chunks) pour éviter d'épuiser la RAM."""
        query = text(f"SELECT * FROM {POSTGRES_TABLE}")
        with self.pg_engine.connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                yield chunk

    def iter_data_from_postgres_clean(self, chunksize: int = 100_000) -> Iterator[pd.DataFrame]:
        """Lit les données déjà filtrées par PostgreSQL (pushdown SQL) en chunks.

        Réduit fortement le volume transféré et la RAM côté Python.
        """
        conds = [
            "passenger_count >= 1",
            "passenger_count <= 8",
            "trip_distance >= 0",
            "trip_distance <= 100",
            "fare_amount >= 0",
            "fare_amount <= 500",
            "tip_amount >= 0",
            "tolls_amount >= 0",
            "total_amount >= 0",
            "tpep_pickup_datetime IS NOT NULL",
            "tpep_dropoff_datetime IS NOT NULL",
        ]
        where_clause = " AND ".join(conds)
        query = text(f"SELECT * FROM {POSTGRES_TABLE} WHERE {where_clause}")
        with self.pg_engine.connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                yield chunk

    # ------------------------------------------------------------------
    # Analyse
    # ------------------------------------------------------------------
    def analyze_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calcule des contrôles qualité simples."""
        stats: Dict[str, Any] = {}

        numeric_cols = [
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]

        negatives = {}
        for col in numeric_cols:
            if col in df.columns:
                negatives[col] = int((df[col] < 0).sum())
        stats["negatives"] = negatives

        nulls = {col: int(df[col].isna().sum()) for col in df.columns}
        stats["nulls"] = nulls

        outliers = {}
        if "passenger_count" in df.columns:
            outliers["passenger_count"] = int(((df["passenger_count"] < 1) | (df["passenger_count"] > 8)).sum())
        if "trip_distance" in df.columns:
            outliers["trip_distance"] = int((df["trip_distance"] > 100).sum())
        if "fare_amount" in df.columns:
            outliers["fare_amount"] = int((df["fare_amount"] > 500).sum())
        stats["outliers"] = outliers

        return stats

    # ------------------------------------------------------------------
    # Nettoyage
    # ------------------------------------------------------------------
    def clean_data(self, df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
        """Applique les règles de nettoyage et retourne un nouveau DataFrame."""
        cleaned = df.copy()

        start_len = int(len(cleaned))
        if verbose:
            print(f"[CLEANER]    Nettoyage: départ {start_len:,} lignes", flush=True)

        # Supprimer valeurs négatives (par colonne)
        for col in [
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]:
            if col in cleaned.columns:
                before = int(len(cleaned))
                cleaned = cleaned[cleaned[col] >= 0]
                removed = before - int(len(cleaned))
                if verbose and removed:
                    print(f"[CLEANER]      - négatifs '{col}': -{removed:,}", flush=True)

        # Règles d'outliers
        if "passenger_count" in cleaned.columns:
            before = int(len(cleaned))
            cleaned = cleaned[(cleaned["passenger_count"] >= 1) & (cleaned["passenger_count"] <= 8)]
            removed = before - int(len(cleaned))
            if verbose and removed:
                print(f"[CLEANER]      - outliers 'passenger_count' (1..8): -{removed:,}", flush=True)

        if "trip_distance" in cleaned.columns:
            before = int(len(cleaned))
            cleaned = cleaned[cleaned["trip_distance"] <= 100]
            removed = before - int(len(cleaned))
            if verbose and removed:
                print(f"[CLEANER]      - outliers 'trip_distance' (>100): -{removed:,}", flush=True)

        if "fare_amount" in cleaned.columns:
            before = int(len(cleaned))
            cleaned = cleaned[cleaned["fare_amount"] <= 500]
            removed = before - int(len(cleaned))
            if verbose and removed:
                print(f"[CLEANER]      - outliers 'fare_amount' (>500): -{removed:,}", flush=True)

        # Dates obligatoires
        required_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        required_dates = [c for c in required_dates if c in cleaned.columns]
        if required_dates:
            before = int(len(cleaned))
            cleaned = cleaned.dropna(subset=required_dates)
            removed = before - int(len(cleaned))
            if verbose and removed:
                print(f"[CLEANER]      - dates manquantes {required_dates}: -{removed:,}", flush=True)

        if verbose:
            print(
                "[CLEANER]    Résumé du nettoyage:",
                {
                    "lignes_avant": start_len,
                    "lignes_apres": int(len(cleaned)),
                    "supprimees": start_len - int(len(cleaned)),
                },
                flush=True,
            )

        return cleaned

    def stream_clean_to_mongodb(self, chunksize: int = 100_000) -> Dict[str, int]:
        """Nettoie et insère en flux dans MongoDB pour limiter l'utilisation mémoire."""
        totals = {
            "lignes_chargees": 0,
            "lignes_conservees": 0,
            "documents_inserts": 0,
            "chunks": 0,
        }

        total_rows = self.get_total_rows()
        processed_rows = 0
        if total_rows:
            print(f"[CLEANER] Total lignes source: {total_rows:,}", flush=True)

        # Remplacer la collection existante une seule fois (supprimer tout)
        current = self.collection.count_documents({})
        if current > 0:
            self.collection.delete_many({})

        # Choisir la source: pushdown SQL ou brut
        use_pushdown = os.getenv("CLEANER_SQL_PUSHDOWN", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
        if use_pushdown:
            print("[CLEANER] Mode: SQL pushdown actif (filtres appliqués côté PostgreSQL)", flush=True)
            source_iter = self.iter_data_from_postgres_clean(chunksize=chunksize)
        else:
            print("[CLEANER] Mode: lecture brute puis nettoyage côté Python", flush=True)
            source_iter = self.iter_data_from_postgres(chunksize=chunksize)

        for i, chunk in enumerate(source_iter, start=1):
            totals["chunks"] += 1
            chunk_len = int(len(chunk))
            totals["lignes_chargees"] += chunk_len
            processed_rows += chunk_len

            print(f"[CLEANER] Chunk {i}: début (taille={chunk_len:,})", flush=True)
            cleaned = self.clean_data(chunk, verbose=True)
            kept_len = int(len(cleaned))
            totals["lignes_conservees"] += kept_len

            if cleaned.empty:
                print(f"[CLEANER] Chunk {i}: aucun enregistrement après nettoyage, skip.", flush=True)
            else:
                # Insertion par mini-lots pour réduire les pics mémoire
                batch_size = int(os.getenv("CLEANER_INSERT_BATCH_SIZE", "5000"))
                ts_cols = [
                    c for c, dtype in cleaned.dtypes.items() if pd.api.types.is_datetime64_any_dtype(dtype)
                ]
                inserted_total = 0
                for start in range(0, kept_len, batch_size):
                    end = min(start + batch_size, kept_len)
                    subset = cleaned.iloc[start:end]
                    records: List[dict] = subset.to_dict(orient="records")
                    for doc in records:
                        for c in ts_cols:
                            val = doc.get(c)
                            if isinstance(val, pd.Timestamp):
                                doc[c] = val.to_pydatetime()
                        if "index" in doc:
                            del doc["index"]
                    res = self.collection.insert_many(records)
                    inserted_total += len(res.inserted_ids)
                totals["documents_inserts"] += inserted_total
                print(
                    f"[CLEANER] Chunk {i}: gardées={kept_len:,}, insérées={inserted_total:,} (batch={batch_size})",
                    flush=True,
                )

            # Avancement global
            if total_rows:
                percent = min(100.0, processed_rows / total_rows * 100.0)
                print(
                    f"[CLEANER] Avancement: {percent:.1f}% ({processed_rows:,}/{total_rows:,})",
                    flush=True,
                )

        return totals

    # ------------------------------------------------------------------
    # Sauvegarde MongoDB (plein)
    # ------------------------------------------------------------------
    def save_to_mongodb(self, df: pd.DataFrame) -> int:
        """Sauvegarde les données nettoyées dans la collection MongoDB."""
        records: List[dict] = []
        timestamp_cols = [
            c
            for c, dtype in df.dtypes.items()
            if pd.api.types.is_datetime64_any_dtype(dtype)
        ]

        for _, row in df.iterrows():
            doc = row.to_dict()
            for c in timestamp_cols:
                val = doc.get(c)
                if isinstance(val, pd.Timestamp):
                    doc[c] = val.to_pydatetime()
            if "index" in doc:
                del doc["index"]
            records.append(doc)

        current_count = self.collection.count_documents({})
        if current_count > 0:
            self.collection.delete_many({})

        if not records:
            return 0

        result = self.collection.insert_many(records)
        return len(result.inserted_ids)

    # ------------------------------------------------------------------
    # Fermeture
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Ferme la connexion MongoDB (l'engine Postgres est géré par le pool)."""
        if self.mongo_client:
            self.mongo_client.close()


def _print_analysis(analysis: Dict[str, Any]) -> None:
    """Affiche un résumé compact de l'analyse pour validation rapide."""
    print("Résumé d'analyse:")
    neg = analysis.get("negatives", {})
    out = analysis.get("outliers", {})
    nulls = analysis.get("nulls", {})
    print("  négatifs:", {k: v for k, v in neg.items() if v})
    print("  outliers:", {k: v for k, v in out.items() if v})
    top_nulls = {k: v for k, v in list(nulls.items())[:5]}
    print("  nulls(extrait):", top_nulls)


if __name__ == "__main__":
    cleaner = DataCleaner()
    try:
        df = cleaner.load_data_from_postgres()
        analysis = cleaner.analyze_data(df)
        _print_analysis(analysis)
        cleaned_df = cleaner.clean_data(df, verbose=True)
        inserted = cleaner.save_to_mongodb(cleaned_df)
        print(f"{inserted} documents insérés dans la collection MongoDB '{MONGO_COLLECTION}'.")
    finally:
        cleaner.close()