"""
Script principal pour importer les données parquet dans PostgreSQL ou DuckDB.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

from import_to_postgres import PostgreSQLImporter
from import_to_duckdb import DuckDBImporter


load_dotenv()


def main():
    raw_data_dir = Path("src/data/raw")
    
    use_postgres = os.getenv("USE_POSTGRES", "true").lower() == "true"
    
    if use_postgres:
        print("Démarrage de l'import vers PostgreSQL...\n")
        importer = PostgreSQLImporter()
    else:
        print("Démarrage de l'import vers DuckDB...\n")
        db_path = "data/taxi_data.duckdb"
        importer = DuckDBImporter(db_path)
    
    # Importer tous les fichiers parquet
    importer.import_all_parquet_files(raw_data_dir)
    
    # Afficher les infos de la table
    importer.get_table_info()
    
    # Fermer la connexion
    importer.close()
    print("\nTerminé !")


if __name__ == "__main__":
    main()

