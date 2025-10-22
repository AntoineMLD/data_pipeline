"""
Gère l'import de fichiers parquet dans une base de données DuckDB.
"""
import duckdb
from pathlib import Path
from datetime import datetime


class DuckDBImporter:
    """
    Classe pour importer des fichiers parquet dans DuckDB avec suivi des imports.
    """
    
    def __init__(self, db_path: str):
        """
        Initialise la connexion à la base de données.
        
        Args:
            db_path: Chemin vers le fichier de base de données DuckDB
        """
        self.db_path = db_path
        self.conn = self._initialize_database()
    
    def _initialize_database(self):
        """
        Crée la connexion et initialise les tables si nécessaire.
        - yellow_taxi_trips : table principale (créée à partir du premier parquet)
        - import_log : table de suivi des imports
        
        Returns:
            Connection DuckDB
        """
        conn = duckdb.connect(self.db_path)
        
        # Créer la table import_log si elle n'existe pas
        conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                file_name TEXT,
                import_date TIMESTAMP,
                rows_imported INTEGER
            )
        """)
        
        return conn
    
    def _create_table_from_first_parquet(self, parquet_file: Path):
        """
        Crée la table yellow_taxi_trips à partir du schéma du premier fichier parquet.
        DuckDB déduit automatiquement le schéma.
        
        Args:
            parquet_file: Chemin vers le fichier parquet
        """
        query = f"""
            CREATE TABLE IF NOT EXISTS yellow_taxi_trips AS 
            SELECT * FROM read_parquet('{parquet_file}') 
            LIMIT 0
            """
        self.conn.execute(query)
        print(f"Table 'yellow_taxi_trips' créée avec le schéma de {parquet_file.name}")
    
    def is_file_imported(self, file_name: str) -> bool:
        """
        Vérifie si un fichier a déjà été importé.
        
        Args:
            file_name: Nom du fichier à vérifier
            
        Returns:
            True si le fichier est déjà importé, False sinon
        """
        result = self.conn.execute(
            "SELECT COUNT(*) FROM import_log WHERE file_name = ?",
            [file_name]
        ).fetchone()
        return result[0] > 0
    
    def import_parquet(self, file_path: Path) -> int:
        """
        Importe un fichier parquet dans la table yellow_taxi_trips.
        Vérifie si déjà importé, compte les lignes et enregistre dans import_log.
        
        Args:
            file_path: Chemin vers le fichier parquet
            
        Returns:
            Nombre de lignes importées (0 si déjà importé)
        """
        # Vérifier si déjà importé
        if self.is_file_imported(file_path.name):
            print(f"{file_path.name} est déjà importé (ignoré)")
            return 0
        
        try:
            # Compter les lignes avant import
            count_before = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()[0]
            
            print(f"Import de {file_path.name}...")
            
            # Importer les données du fichier parquet
            query = f"""
                INSERT INTO yellow_taxi_trips 
                SELECT * FROM read_parquet('{file_path}')
            """
            self.conn.execute(query)
            
            # Compter les lignes après import
            count_after = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()[0]
            
            rows_imported = count_after - count_before
            
            # Enregistrer dans import_log
            self.conn.execute("""
                INSERT INTO import_log (file_name, import_date, rows_imported)
                VALUES (?, ?, ?)
            """, [file_path.name, datetime.now(), rows_imported])
            
            print(f"{rows_imported:,} lignes importées depuis {file_path.name}")
            return rows_imported
            
        except Exception as e:
            print(f"Erreur lors de l'import de {file_path.name}: {e}")
            return 0
    
    def import_all_parquet_files(self, raw_dir: Path) -> int:
        """
        Importe tous les fichiers parquet d'un répertoire.
        
        Args:
            raw_dir: Chemin vers le répertoire contenant les fichiers parquet
            
        Returns:
            Nombre total de lignes importées
        """
        # Lister tous les fichiers parquet
        parquet_files = sorted(raw_dir.glob("*.parquet"))
        
        if not parquet_files:
            print(f"Aucun fichier parquet trouvé dans {raw_dir}")
            return 0
        
        # Créer la table à partir du premier fichier
        self._create_table_from_first_parquet(parquet_files[0])
        
        # Importer tous les fichiers
        total_rows = 0
        print(f"\nImport de {len(parquet_files)} fichier(s)...\n")
        
        for file_path in parquet_files:
            rows = self.import_parquet(file_path)
            total_rows += rows
        
        print(f"\nImport terminé : {total_rows:,} lignes importées au total")
        return total_rows
    
    def get_table_info(self):
        """
        Affiche les informations de base sur la table yellow_taxi_trips.
        """
        try:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()
            print(f"\nNombre de lignes dans yellow_taxi_trips: {result[0]:,}")
        except Exception as e:
            print(f"Erreur: {e}")
    
    def get_statistics(self):
        """
        Affiche des statistiques détaillées sur les imports.
        """
        print("\nStatistiques détaillées:")
        print("-" * 50)
        
        try:
            # Nombre total de trajets
            result = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()
            print(f"Nombre total de trajets: {result[0]:,}")
            
            # Nombre de fichiers importés
            result = self.conn.execute(
                "SELECT COUNT(*) FROM import_log"
            ).fetchone()
            print(f"Nombre de fichiers importés: {result[0]}")
            
            # Plage de dates d'import
            result = self.conn.execute(
                "SELECT MIN(import_date), MAX(import_date) FROM import_log"
            ).fetchone()
            if result[0]:
                print(f"Premier import: {result[0]}")
                print(f"Dernier import: {result[1]}")
            
            # Total de lignes importées
            result = self.conn.execute(
                "SELECT SUM(rows_imported) FROM import_log"
            ).fetchone()
            print(f"Total lignes importées: {result[0]:,}")
            
        except Exception as e:
            print(f"Erreur lors de la récupération des statistiques: {e}")
    
    def close(self):
        """
        Ferme la connexion à la base de données.
        """
        self.conn.close()
        print("\nConnexion fermée")


if __name__ == "__main__":
    importer = DuckDBImporter("src/data/taxi_data.duckdb")
    importer.import_all_parquet_files(Path("src/data/raw"))
    importer.get_table_info()
    importer.get_statistics()
    importer.close()
