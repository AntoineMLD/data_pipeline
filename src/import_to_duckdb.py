# File: src/import_to_duckdb.py
from __future__ import annotations 

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb


@dataclass
class ImportStats:
    """Conteneur simple pour les statistiques globales."""
    total_trips: int
    imported_files: int
    min_pickup: Optional[datetime]
    max_pickup: Optional[datetime]
    db_size_bytes: int


class DuckDBImporter:
    """
    Import minimaliste des Parquet Yellow Taxi dans DuckDB avec journal des imports.
    Points clés:
      - 'import_log' empêche les doublons (clé primaire = nom de fichier).
      - 'read_parquet' côté DuckDB (pas de pandas).
      - Insertion 'BY NAME' → mappe les colonnes par nom (ordre toléré).
    """

    def __init__(self, db_path: str = "nyc_taxi.duckdb") -> None:
        """Ouvre/Crée la base, puis garantit le schéma."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_database()

    def _initialize_database(self) -> None:
        """
        Crée les tables si besoin et assure la présence de la colonne manquante.
        Pourquoi:
          - Le schéma TLC 2025 inclut 'cbd_congestion_fee' → 20 colonnes.
          - 'BY NAME' évite les soucis d'ordre, mais il faut toutes les colonnes.
        """
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                VendorID BIGINT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count DOUBLE,
                trip_distance DOUBLE,
                RatecodeID DOUBLE,
                store_and_fwd_flag VARCHAR,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                payment_type BIGINT,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                congestion_surcharge DOUBLE,
                Airport_fee DOUBLE,
                cbd_congestion_fee DOUBLE
            )
            """
        )
        # Si la table existait en 19 colonnes, ajoute la colonne manquante.
        self.conn.execute(
            "ALTER TABLE yellow_taxi_trips ADD COLUMN IF NOT EXISTS cbd_congestion_fee DOUBLE"
        )

        # Journal des imports (évite doublons).
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS import_log (
                file_name VARCHAR PRIMARY KEY,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_imported BIGINT
            )
            """
        )

    def is_file_imported(self, filename: str) -> bool:
        """Retourne True si 'filename' est déjà dans import_log (skip rapide)."""
        row = self.conn.execute(
            "SELECT 1 FROM import_log WHERE file_name = ? LIMIT 1", [filename]
        ).fetchone()
        return row is not None

    def import_parquet(self, file_path: Path) -> bool:
        """
        Importe un .parquet dans 'yellow_taxi_trips' en une transaction.
        True si nouvel import OK, False si skip ou erreur.
        """
        filename = file_path.name

        if self.is_file_imported(filename):
            print(f"[skip] {filename} déjà importé")
            return False  # pas un nouvel import

        try:
            self.conn.execute("BEGIN")
            before_count = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()[0]

            # BY NAME : associe les colonnes par nom (ordre flexible).
            self.conn.execute(
                "INSERT INTO yellow_taxi_trips BY NAME SELECT * FROM read_parquet(?)",
                [str(file_path)],
            )

            after_count = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()[0]
            rows_imported = int(after_count) - int(before_count)

            self.conn.execute(
                "INSERT INTO import_log(file_name, rows_imported) VALUES (?, ?)",
                [filename, rows_imported],
            )
            self.conn.execute("COMMIT")
            print(f"[ok] {filename} importé ({rows_imported} lignes)")
            return True

        except Exception as e:
            # État cohérent garanti: tout ou rien.
            try:
                self.conn.execute("ROLLBACK")
            except Exception:
                pass
            print(f"[error] {filename}: {e}")
            return False

    def import_all_parquet_files(self, data_dir: Path = Path("src/data/raw")) -> int:
        """
        Parcourt 'data_dir' (trié) et importe chaque .parquet.
        Retourne le nombre de fichiers réellement importés (exclut les skips).
        """
        if not data_dir.exists():
            print(f"[info] Dossier introuvable: {data_dir.resolve()}")
            return 0

        files = sorted(data_dir.glob("*.parquet"))
        if not files:
            print(f"[info] Aucun .parquet dans {data_dir.resolve()}")
            return 0

        imported = 0
        print(f"[start] {len(files)} fichier(s) détecté(s) dans {data_dir.resolve()}")
        for i, f in enumerate(files, 1):
            print(f"[{i}/{len(files)}] Traitement: {f.name}")
            if self.import_parquet(f):  # True → nouvel import
                imported += 1
        return imported

    def get_statistics(self) -> ImportStats:
        """
        Renvoie:
          - total trajets,
          - nombre de fichiers importés,
          - min/max sur tpep_pickup_datetime,
          - taille du fichier .duckdb.
        """
        total_trips = self.conn.execute(
            "SELECT COUNT(*) FROM yellow_taxi_trips"
        ).fetchone()[0]

        imported_files = self.conn.execute(
            "SELECT COUNT(*) FROM import_log"
        ).fetchone()[0]

        min_pickup, max_pickup = self.conn.execute(
            "SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips"
        ).fetchone()

        db_size = Path(self.db_path).stat().st_size if Path(self.db_path).exists() else 0

        return ImportStats(
            total_trips=int(total_trips),
            imported_files=int(imported_files),
            min_pickup=min_pickup,
            max_pickup=max_pickup,
            db_size_bytes=int(db_size),
        )

    def close(self) -> None:
        """Ferme la connexion DuckDB (propre)."""
        try:
            self.conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    """
    Exemples:
      uv run python src/import_to_duckdb.py
      uv run python src/import_to_duckdb.py /chemin/ma.db /autre/dossier
    """
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else "nyc_taxi.duckdb"
    data_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("src/data/raw")

    importer = DuckDBImporter(db_path=db_path)
    try:
        count = importer.import_all_parquet_files(data_dir=data_dir)
        stats = importer.get_statistics()
        print("\n[summary]")
        print(f"  ✓ Fichiers importés      : {count}")
        print(f"  ✓ Fichiers en base (log) : {stats.imported_files}")
        print(f"  ✓ Total trajets          : {stats.total_trips}")
        print(f"  ✓ Plage de dates         : {stats.min_pickup} → {stats.max_pickup}")
        print(f"  ✓ Taille DB              : {stats.db_size_bytes:,} bytes")
    finally:
        importer.close()
