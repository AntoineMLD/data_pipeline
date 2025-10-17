"""
Script principal pour importer les données parquet dans DuckDB.
"""
from import_to_duckdb import DuckDBImporter


def main():
    # Chemins
    db_path = "data/taxi_data.duckdb"
    raw_data_dir = "src/data/raw"
    
    # Créer l'importeur
    importer = DuckDBImporter(db_path)
    
    # Importer tous les fichiers parquet
    print("🚀 Démarrage de l'import...\n")
    importer.import_all_parquet_files(raw_data_dir)
    
    # Afficher les infos de la table
    importer.get_table_info()
    
    # Fermer la connexion
    importer.close()
    print("\n✅ Terminé !")


if __name__ == "__main__":
    main()

