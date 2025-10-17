import duckdb
import pandas as pd
from pathlib import Path

class DuckDBImporter:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = self._initialize_database()
    
    def _initialize_database(self):
        """ Créer la table yellow_taxi_trips si elle n'existe pas .
        créer la table import_log pour tracker les imports.
        La table import_log contient : file_name, import_date, rows_imported. """
        conn = duckdb.connect(self.db_path)
        if not conn.table_exists("yellow_taxi_trips"):
            conn.execute("""CREATE TABLE yeallo_taxi_trips (""")
            conn.execute("""CREATE TABLE import_log (
                file_name TEXT,
                import_date TIMESTAMP,
                rows_imported INTEGER
            )""")
        return conn
    
    def is_file_imported(self, file_name: str) -> bool:

