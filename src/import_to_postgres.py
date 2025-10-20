"""
Gère l'import de fichiers parquet dans une base de données PostgreSQL avec SQLAlchemy.
"""
from pathlib import Path
from datetime import datetime
from typing import Union
from io import StringIO

from sqlalchemy import Column, Integer, DateTime, Text, inspect, text
from sqlalchemy.orm import Session
import pandas as pd
import psycopg2

from database import engine, SessionLocal, Base, get_database_url


class ImportLog(Base):
    """
    Table de suivi des fichiers importés.
    """
    __tablename__ = "import_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(Text)
    import_date = Column(DateTime)
    rows_imported = Column(Integer)


class PostgreSQLImporter:
    """
    Classe pour importer des fichiers parquet dans PostgreSQL avec suivi des imports.
    """
    
    def __init__(self):
        """
        Initialise la connexion à la base de données PostgreSQL.
        """
        self.engine = engine
        self._initialize_database()
    
    def _initialize_database(self):
        """
        Crée les tables si elles n'existent pas.
        - yellow_taxi_trips : table principale (créée à partir du premier parquet)
        - import_log : table de suivi des imports
        """
        # Créer la table import_log
        Base.metadata.create_all(bind=self.engine, tables=[ImportLog.__table__])
    
    def _create_table_from_first_parquet(self, parquet_file: Path):
        """
        Crée la table yellow_taxi_trips à partir du schéma du premier fichier parquet.
        Utilise pandas pour détecter le schéma et le convertir en types PostgreSQL.
        
        Args:
            parquet_file: Chemin vers le fichier parquet
        """
        # Pourquoi inspect: vérifie si la table existe déjà.
        inspector = inspect(self.engine)
        if "yellow_taxi_trips" in inspector.get_table_names():
            print(f"Table 'yellow_taxi_trips' existe déjà")
            return
        
        # Lire un échantillon pour obtenir le schéma
        df_sample = pd.read_parquet(parquet_file, engine='pyarrow')
        
        # Pourquoi to_sql avec 0 lignes: crée la table vide avec le bon schéma.
        # Pourquoi if_exists='fail': évite d'écraser une table existante par erreur.
        df_sample.head(0).to_sql(
            'yellow_taxi_trips',
            self.engine,
            if_exists='fail',
            index=False
        )
        
        print(f"Table 'yellow_taxi_trips' créée avec le schéma de {parquet_file.name}")
    
    def _bulk_insert_with_copy(self, df: pd.DataFrame):
        """
        Import bulk ultra-rapide avec COPY FROM de PostgreSQL.
        Pourquoi: 10x plus rapide que to_sql pour les gros volumes.
        
        Args:
            df: DataFrame pandas à importer
        """
        # Convertir DataFrame en CSV en mémoire
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        # Connexion psycopg2 directe pour COPY FROM
        db_url = get_database_url()
        # Parser l'URL SQLAlchemy pour psycopg2
        import re
        match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', db_url)
        if not match:
            raise ValueError(f"URL invalide: {db_url}")
        
        user, password, host, port, database = match.groups()
        
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password
        )
        
        try:
            with conn.cursor() as cur:
                cur.copy_from(
                    buffer,
                    'yellow_taxi_trips',
                    columns=df.columns.tolist(),
                    sep='\t',
                    null='\\N'
                )
            conn.commit()
        finally:
            conn.close()
    
    def is_file_imported(self, file_name: str) -> bool:
        """
        Vérifie si un fichier a déjà été importé.
        
        Args:
            file_name: Nom du fichier à vérifier
            
        Returns:
            True si le fichier est déjà importé, False sinon
        """
        db: Session = SessionLocal()
        try:
            count = db.query(ImportLog).filter(ImportLog.file_name == file_name).count()
            return count > 0
        finally:
            db.close()
    
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
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                count_before = result.scalar()
            
            print(f"Import de {file_path.name}...")
            
            # Lire le parquet avec pandas
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            # Pourquoi COPY FROM: méthode la plus rapide pour PostgreSQL (~10x plus rapide que to_sql).
            # On utilise psycopg2 directement pour le bulk loading, SQLAlchemy pour le reste.
            self._bulk_insert_with_copy(df)
            
            # Compter les lignes après import
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                count_after = result.scalar()
            
            rows_imported = count_after - count_before
            
            # Enregistrer dans import_log
            db: Session = SessionLocal()
            try:
                log_entry = ImportLog(
                    file_name=file_path.name,
                    import_date=datetime.now(),
                    rows_imported=rows_imported
                )
                db.add(log_entry)
                db.commit()
            finally:
                db.close()
            
            print(f"{rows_imported:,} lignes importées depuis {file_path.name}")
            return rows_imported
            
        except Exception as e:
            print(f"Erreur lors de l'import de {file_path.name}: {e}")
            return 0
    
    def import_all_parquet_files(self, raw_dir: Union[Path, str]) -> int:
        """
        Importe tous les fichiers parquet d'un répertoire.
        
        Args:
            raw_dir: Chemin vers le répertoire contenant les fichiers parquet
            
        Returns:
            Nombre total de lignes importées
        """
        raw_dir = Path(raw_dir)
        
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
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                count = result.scalar()
                print(f"\nNombre de lignes dans yellow_taxi_trips: {count:,}")
        except Exception as e:
            print(f"Erreur: {e}")
    
    def get_statistics(self):
        """
        Affiche des statistiques détaillées sur les imports.
        """
        print("\nStatistiques détaillées:")
        print("-" * 50)
        
        try:
            db: Session = SessionLocal()
            try:
                # Nombre total de trajets
                with self.engine.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                    count = result.scalar()
                    print(f"Nombre total de trajets: {count:,}")
                
                # Nombre de fichiers importés
                count = db.query(ImportLog).count()
                print(f"Nombre de fichiers importés: {count}")
                
                # Plage de dates d'import
                from sqlalchemy import func
                result = db.query(
                    func.min(ImportLog.import_date),
                    func.max(ImportLog.import_date)
                ).first()
                
                if result[0]:
                    print(f"Premier import: {result[0]}")
                    print(f"Dernier import: {result[1]}")
                
                # Total de lignes importées
                total = db.query(func.sum(ImportLog.rows_imported)).scalar()
                if total:
                    print(f"Total lignes importées: {total:,}")
                    
            finally:
                db.close()
                
        except Exception as e:
            print(f"Erreur lors de la récupération des statistiques: {e}")
    
    def close(self):
        """
        Ferme la connexion à la base de données.
        """
        self.engine.dispose()
        print("\nConnexion fermée")


if __name__ == "__main__":
    importer = PostgreSQLImporter()
    importer.import_all_parquet_files(Path("src/data/raw"))
    importer.get_table_info()
    importer.get_statistics()
    importer.close()
