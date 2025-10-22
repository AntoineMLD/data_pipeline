"""
═══════════════════════════════════════════════════════════════════════════════
MODULE D'IMPORT DES DONNÉES NYC TAXI DANS POSTGRESQL
═══════════════════════════════════════════════════════════════════════════════

Ce module gère l'import des fichiers Parquet NYC Yellow Taxi dans PostgreSQL
avec des optimisations de performance (COPY FROM) et un système de traçabilité.

ARCHITECTURE:
    1. Téléchargement des fichiers Parquet (depuis download_data.py)
    2. Création automatique du schéma PostgreSQL (premier fichier)
    3. Import par chunks avec COPY FROM (10x plus rapide que INSERT classique)
    4. Traçabilité via table import_log (évite les doublons)

PERFORMANCE:
    - COPY FROM (psycopg2): ~1 million de lignes/seconde
    - INSERT classique: ~100k lignes/seconde
    - Gain: 10x plus rapide !

IDEMPOTENCE:
    - Peut être relancé sans créer de doublons
    - Vérifie import_log avant chaque import

USAGE:
    importer = PostgreSQLImporter()
    importer.import_all_parquet_files(Path("src/data/raw"))
    importer.close()
"""

# ═══════════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════════

from pathlib import Path
from datetime import datetime
from typing import Union
from io import StringIO

# SQLAlchemy: ORM pour Python (comme Hibernate en Java)
from sqlalchemy import Column, Integer, DateTime, Text, inspect, text
from sqlalchemy.orm import Session

# Pandas: manipulation de données tabulaires (comme Excel mais en code)
import pandas as pd

# PyArrow: lecture ultra-rapide des fichiers Parquet
import pyarrow.parquet as pq

# psycopg2: driver PostgreSQL bas niveau (pour COPY FROM)
import psycopg2

# Nos modules de configuration
from database import engine, SessionLocal, Base, get_database_url


# ═══════════════════════════════════════════════════════════════════════════
# MODÈLE ORM: TABLE IMPORT_LOG (Traçabilité des imports)
# ═══════════════════════════════════════════════════════════════════════════

class ImportLog(Base):
    """
    Table de suivi des fichiers déjà importés.
    
    RÔLE:
        Évite de réimporter deux fois le même fichier Parquet.
        Permet de relancer le script sans créer de doublons.
    
    COLONNES:
        - id: Clé primaire auto-incrémentée
        - file_name: Nom du fichier Parquet (ex: "yellow_tripdata_2025-01.parquet")
        - import_date: Date/heure de l'import
        - rows_imported: Nombre de lignes importées depuis ce fichier
    
    EXEMPLE D'UTILISATION:
        # Vérifier si un fichier a déjà été importé
        existe = db.query(ImportLog).filter(ImportLog.file_name == "yellow_tripdata_2025-01.parquet").count() > 0
    """
    __tablename__ = "import_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(Text)                # Nom du fichier Parquet
    import_date = Column(DateTime)          # Date/heure de l'import
    rows_imported = Column(Integer)         # Nombre de lignes importées


# ═══════════════════════════════════════════════════════════════════════════
# CLASSE PRINCIPALE: PostgreSQLImporter
# ═══════════════════════════════════════════════════════════════════════════

class PostgreSQLImporter:
    """
    Classe responsable de l'import des fichiers Parquet dans PostgreSQL.
    
    ARCHITECTURE:
        Cette classe suit le principe de responsabilité unique (SOLID):
        - Elle gère UNIQUEMENT l'import PostgreSQL
        - Elle ne télécharge PAS les fichiers (rôle de download_data.py)
        - Elle ne nettoie PAS les données (rôle de data_cleaner.py)
    
    MÉTHODES PRINCIPALES:
        - import_all_parquet_files(): Point d'entrée principal
        - import_parquet(): Import d'un fichier unique
        - _bulk_insert_with_copy(): Optimisation COPY FROM (10x plus rapide)
    
    MÉTHODES PRIVÉES (helpers):
        - _initialize_database(): Crée la table import_log
        - _create_table_from_first_parquet(): Crée yellow_taxi_trips
        - is_file_imported(): Vérifie si un fichier a déjà été importé
    
    EXEMPLE D'UTILISATION:
        >>> importer = PostgreSQLImporter()
        >>> importer.import_all_parquet_files(Path("src/data/raw"))
        >>> importer.get_statistics()
        >>> importer.close()
    """
    
    def __init__(self):
        """
        Initialise la connexion à PostgreSQL.
        
        ÉTAPES:
            1. Récupère l'engine SQLAlchemy depuis database.py
            2. Crée la table import_log si elle n'existe pas
        
        NOTES:
            - L'engine est un pool de connexions (évite de créer/fermer à chaque requête)
            - pool_pre_ping=True vérifie que la connexion est vivante avant de l'utiliser
        """
        self.engine = engine
        self._initialize_database()
    
    # ═══════════════════════════════════════════════════════════════════════
    # INITIALISATION DE LA BASE DE DONNÉES
    # ═══════════════════════════════════════════════════════════════════════
    
    def _initialize_database(self):
        """
        Crée les tables nécessaires si elles n'existent pas.
        
        TABLES CRÉÉES:
            - import_log: Table de traçabilité (créée ici)
            - yellow_taxi_trips: Table principale (créée plus tard depuis le premier parquet)
        
        POURQUOI 2 ÉTAPES SÉPARÉES?
            - import_log: Structure fixe, définie dans ce fichier (modèle ORM)
            - yellow_taxi_trips: Structure variable, détectée depuis le fichier Parquet
        
        IDEMPOTENCE:
            - CREATE TABLE IF NOT EXISTS: Ne fait rien si la table existe déjà
            - Peut être appelé plusieurs fois sans erreur
        """
        # Créer uniquement la table import_log (la structure est définie dans ImportLog)
        Base.metadata.create_all(bind=self.engine, tables=[ImportLog.__table__])
    
    # ═══════════════════════════════════════════════════════════════════════
    # CRÉATION AUTOMATIQUE DU SCHÉMA PostgreSQL
    # ═══════════════════════════════════════════════════════════════════════
    
    def _create_table_from_first_parquet(self, parquet_file: Path):
        """
        Crée la table yellow_taxi_trips en détectant automatiquement le schéma.
        
        POURQUOI CETTE APPROCHE?
            Les fichiers Parquet NYC Taxi peuvent évoluer (nouvelles colonnes).
            Au lieu de hardcoder 20 colonnes, on détecte le schéma automatiquement.
        
        COMMENT ÇA FONCTIONNE?
            1. Lire le premier fichier Parquet avec pandas
            2. Extraire uniquement les noms et types de colonnes (df.head(0))
            3. pandas.to_sql() génère le CREATE TABLE automatiquement
            4. Une colonne 'index' est ajoutée comme PRIMARY KEY
        
        MAPPING DES TYPES (pandas → PostgreSQL):
            - int64 → BIGINT
            - float64 → DOUBLE PRECISION
            - datetime64[ns] → TIMESTAMP
            - object (string) → TEXT
        
        Args:
            parquet_file: Chemin vers le premier fichier Parquet
        
        Returns:
            None (la table est créée dans PostgreSQL)
        
        EXEMPLE:
            Parquet contient: VendorID (int), fare_amount (float), pickup_datetime (datetime)
            → PostgreSQL reçoit: vendorid BIGINT, fare_amount DOUBLE PRECISION, pickup_datetime TIMESTAMP
        """
        # Étape 1: Vérifier si la table existe déjà
        inspector = inspect(self.engine)
        if "yellow_taxi_trips" in inspector.get_table_names():
            print(f"[OK] Table 'yellow_taxi_trips' existe déjà", flush=True)
            return
        
        print(f"Création de la table 'yellow_taxi_trips'...", flush=True)
        
        # Étape 2: Lire le premier fichier Parquet pour obtenir le schéma
        df_sample = pd.read_parquet(parquet_file, engine='pyarrow')
        print(f"   Schéma détecté: {len(df_sample.columns)} colonnes", flush=True)
        
        # Étape 3: Créer la table VIDE avec le bon schéma
        # head(0) = DataFrame vide mais avec les colonnes et types
        # to_sql() génère automatiquement le CREATE TABLE
        df_sample.head(0).to_sql(
            'yellow_taxi_trips',    # Nom de la table
            self.engine,            # Connexion PostgreSQL
            if_exists='fail',       # Échoue si la table existe (sécurité)
            index=True,             # Crée une colonne 'index' comme PRIMARY KEY
            index_label='index'     # Nom de la colonne PRIMARY KEY
        )
        
        print(f"[OK] Table 'yellow_taxi_trips' créée avec succès", flush=True)
    
    # ═══════════════════════════════════════════════════════════════════════
    # OPTIMISATION: COPY FROM (10x PLUS RAPIDE QUE INSERT)
    # ═══════════════════════════════════════════════════════════════════════
    
    def _bulk_insert_with_copy(self, df: pd.DataFrame):
        """
        Import ultra-rapide avec la commande COPY FROM de PostgreSQL.
        
        POURQUOI COPY FROM EST 10x PLUS RAPIDE?
        
        Méthode classique (lente):
            INSERT INTO table VALUES (1, 'a', 100);  -- 1 ligne
            INSERT INTO table VALUES (2, 'b', 200);  -- 1 ligne
            INSERT INTO table VALUES (3, 'c', 300);  -- 1 ligne
            → 100,000 requêtes SQL = ~10 secondes
        
        Méthode COPY FROM (rapide):
            COPY table FROM stdin;
            1\ta\t100
            2\tb\t200
            3\tc\t300
            → 1 seule requête = ~1 seconde
        
        COMMENT ÇA FONCTIONNE?
            1. Convertir le DataFrame en CSV en mémoire (buffer StringIO)
            2. Utiliser psycopg2 (driver bas niveau) pour COPY FROM
            3. PostgreSQL lit le CSV en streaming ultra-rapide
        
        POURQUOI psycopg2 AU LIEU DE SQLAlchemy?
            SQLAlchemy ne supporte pas COPY FROM (API trop haut niveau).
            On doit descendre au niveau du driver psycopg2.
        
        Args:
            df: DataFrame pandas à importer (peut contenir 100k+ lignes)
        
        Returns:
            None (les données sont insérées dans PostgreSQL)
        
        PERFORMANCE:
            - 100,000 lignes en ~1 seconde
            - 1,000,000 lignes en ~10 secondes
            - Vs INSERT classique: 10x plus lent
        """
        print(f"      Import via COPY FROM (optimisé)...", flush=True)
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 1: Convertir DataFrame → CSV en mémoire
        # ─────────────────────────────────────────────────────────────────
        buffer = StringIO()
        df.to_csv(
            buffer,
            index=True,         # Inclure la colonne 'index' (PRIMARY KEY)
            header=False,       # Pas de ligne d'en-tête (on a déjà les colonnes)
            sep='\t',           # Séparateur TAB (standard COPY FROM)
            na_rep='\\N'        # NULL représenté par \N (standard PostgreSQL)
        )
        buffer.seek(0)  # Revenir au début du buffer pour le lire
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 2: Parser l'URL SQLAlchemy pour extraire les credentials
        # ─────────────────────────────────────────────────────────────────
        db_url = get_database_url()
        # Format: postgresql://user:password@host:port/database
        import re
        match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', db_url)
        if not match:
            raise ValueError(f"URL PostgreSQL invalide: {db_url}")
        
        user, password, host, port, database = match.groups()
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 3: Connexion psycopg2 directe (bas niveau)
        # ─────────────────────────────────────────────────────────────────
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password
        )
        
        try:
            with conn.cursor() as cur:
                # Étape 4: Exécuter COPY FROM
                columns = ['index'] + df.columns.tolist()
                
                # COPY table_name (col1, col2, col3) FROM stdin WITH (FORMAT csv, DELIMITER '\t', NULL '\N')
                cur.copy_from(
                    buffer,                     # Source: notre CSV en mémoire
                    'yellow_taxi_trips',        # Table cible
                    columns=columns,            # Liste des colonnes
                    sep='\t',                   # Séparateur TAB
                    null='\\N'                  # Représentation de NULL
                )
            
            # Commit explicite (pas d'autocommit)
            conn.commit()
            print(f"      [OK] COPY FROM terminé", flush=True)
            
        finally:
            # Toujours fermer la connexion (même en cas d'erreur)
            conn.close()
    
    # ═══════════════════════════════════════════════════════════════════════
    # IDEMPOTENCE: Éviter les doublons
    # ═══════════════════════════════════════════════════════════════════════
    
    def is_file_imported(self, file_name: str) -> bool:
        """
        Vérifie si un fichier Parquet a déjà été importé.
        
        POURQUOI CETTE VÉRIFICATION?
            Permet de relancer le script sans réimporter les mêmes données.
            Utile en cas d'erreur réseau, crash, ou ajout de nouveaux fichiers.
        
        COMMENT?
            Requête SQL: SELECT COUNT(*) FROM import_log WHERE file_name = ?
            Si COUNT > 0, le fichier a déjà été importé.
        
        Args:
            file_name: Nom du fichier Parquet (ex: "yellow_tripdata_2025-01.parquet")
        
        Returns:
            True si déjà importé, False sinon
        
        EXEMPLE:
            >>> importer.is_file_imported("yellow_tripdata_2025-01.parquet")
            True   # Ce fichier a déjà été importé
            >>> importer.is_file_imported("yellow_tripdata_2025-12.parquet")
            False  # Ce fichier n'a jamais été importé
        """
        db: Session = SessionLocal()
        try:
            # Requête SQL via SQLAlchemy ORM
            count = db.query(ImportLog).filter(ImportLog.file_name == file_name).count()
            return count > 0
        finally:
            # Toujours fermer la session (libère la connexion dans le pool)
            db.close()
    
    # ═══════════════════════════════════════════════════════════════════════
    # IMPORT D'UN FICHIER PARQUET (par chunks)
    # ═══════════════════════════════════════════════════════════════════════
    
    def import_parquet(self, file_path: Path, chunk_size: int = 100_000) -> int:
        """
        Importe un fichier Parquet dans PostgreSQL par chunks (morceaux).
        
        POURQUOI PAR CHUNKS?
            Un fichier Parquet contient ~3 millions de lignes = ~500 MB en RAM.
            Tout charger d'un coup = risque de manquer de mémoire.
            Solution: Lire par morceaux de 100k lignes = ~15 MB par chunk.
        
        FLUX D'IMPORT:
            1. Vérifier si déjà importé (table import_log)
            2. Compter les lignes actuelles (COUNT avant import)
            3. Lire le Parquet par chunks de 100k lignes
            4. Pour chaque chunk: COPY FROM vers PostgreSQL
            5. Compter les lignes finales (COUNT après import)
            6. Enregistrer dans import_log
        
        CHUNK_SIZE = 100,000 LIGNES:
            - Trop petit (ex: 10k) = trop de requêtes COPY FROM = lent
            - Trop grand (ex: 1M) = trop de RAM = risque de crash
            - 100k = bon équilibre entre vitesse et RAM
        
        Args:
            file_path: Chemin vers le fichier Parquet
            chunk_size: Nombre de lignes par chunk (défaut: 100,000)
        
        Returns:
            Nombre de lignes importées (0 si déjà importé)
        
        EXEMPLE:
            Fichier avec 2,964,624 lignes:
            → 30 chunks de 100k lignes
            → ~30 secondes d'import total
        """
        # ─────────────────────────────────────────────────────────────────
        # Étape 1: Vérifier si déjà importé (idempotence)
        # ─────────────────────────────────────────────────────────────────
        if self.is_file_imported(file_path.name):
            print(f"     [SKIP] Déjà importé", flush=True)
            return 0
        
        try:
            # ─────────────────────────────────────────────────────────────────
            # Étape 2: Compter les lignes AVANT import (pour calculer le delta)
            # ─────────────────────────────────────────────────────────────────
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                count_before = result.scalar()
            
            import sys
            print(f"     Chunks de {chunk_size:,} lignes", flush=True)
            
            # ─────────────────────────────────────────────────────────────────
            # Étape 3: Ouvrir le fichier Parquet et calculer le nombre de chunks
            # ─────────────────────────────────────────────────────────────────
            parquet_file = pq.ParquetFile(file_path)
            total_rows = parquet_file.metadata.num_rows
            num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Division arrondie supérieure
            
            print(f"     {total_rows:,} lignes à importer en {num_chunks} chunk(s)", flush=True)
            sys.stdout.flush()
            
            # ─────────────────────────────────────────────────────────────────
            # Étape 4: Lire et importer chunk par chunk
            # ─────────────────────────────────────────────────────────────────
            # iter_batches() = itérateur qui lit le Parquet par morceaux
            # batch_size = nombre de lignes par morceau
            for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
                # Convertir le batch PyArrow en DataFrame pandas
                df_chunk = batch.to_pandas()
                
                # Import ultra-rapide avec COPY FROM
                self._bulk_insert_with_copy(df_chunk)
                
                # Afficher la progression
                progress = (i + 1) / num_chunks * 100
                print(f"     Chunk {i+1}/{num_chunks} importé ({progress:.0f}%)", flush=True)
                sys.stdout.flush()
            
            # ─────────────────────────────────────────────────────────────────
            # Étape 5: Compter les lignes APRÈS import (pour vérifier)
            # ─────────────────────────────────────────────────────────────────
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                count_after = result.scalar()
            
            rows_imported = count_after - count_before
            
            # ─────────────────────────────────────────────────────────────────
            # Étape 6: Enregistrer dans import_log (traçabilité)
            # ─────────────────────────────────────────────────────────────────
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
            
            print(f"     [OK] {rows_imported:,} lignes importées depuis {file_path.name}")
            return rows_imported
            
        except Exception as e:
            print(f"     [ERREUR] Échec de l'import de {file_path.name}: {e}")
            return 0
    
    # ═══════════════════════════════════════════════════════════════════════
    # ORCHESTRATION: Importer tous les fichiers Parquet
    # ═══════════════════════════════════════════════════════════════════════
    
    def import_all_parquet_files(self, raw_dir: Union[Path, str]) -> int:
        """
        Point d'entrée principal: importe tous les fichiers Parquet d'un répertoire.
        
        FLUX COMPLET:
            1. Scanner le répertoire (chercher tous les *.parquet)
            2. Créer la table yellow_taxi_trips depuis le premier fichier
            3. Pour chaque fichier Parquet:
               a. Vérifier si déjà importé
               b. Si non importé: lire par chunks et importer
               c. Enregistrer dans import_log
            4. Afficher les statistiques finales
        
        ROBUSTESSE:
            - Si un fichier échoue, les autres continuent
            - Si relancé, skip les fichiers déjà importés
            - Affichage en temps réel (flush=True pour Docker logs)
        
        Args:
            raw_dir: Chemin vers le répertoire contenant les fichiers Parquet
                    Exemple: "src/data/raw"
        
        Returns:
            Nombre total de nouvelles lignes importées
        
        EXEMPLE D'UTILISATION:
            >>> importer = PostgreSQLImporter()
            >>> total = importer.import_all_parquet_files("src/data/raw")
            >>> print(f"Import terminé: {total:,} lignes")
            Import terminé: 23,650,112 lignes
        """
        import sys
        raw_dir = Path(raw_dir)
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 1: Scanner le répertoire pour trouver les fichiers Parquet
        # ─────────────────────────────────────────────────────────────────
        parquet_files = sorted(raw_dir.glob("*.parquet"))
        
        if not parquet_files:
            print(f"[ERREUR] Aucun fichier parquet trouvé dans {raw_dir}", flush=True)
            return 0
        
        # En-tête de démarrage
        print(f"\n{'='*70}", flush=True)
        print(f"IMPORT DES DONNÉES NYC TAXI DANS POSTGRESQL", flush=True)
        print(f"{'='*70}", flush=True)
        print(f"Répertoire source: {raw_dir}", flush=True)
        print(f"Fichiers Parquet trouvés: {len(parquet_files)}", flush=True)
        print(f"{'='*70}\n", flush=True)
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 2: Créer la table yellow_taxi_trips (depuis le 1er fichier)
        # ─────────────────────────────────────────────────────────────────
        self._create_table_from_first_parquet(parquet_files[0])
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 3: Importer tous les fichiers un par un
        # ─────────────────────────────────────────────────────────────────
        total_rows = 0
        
        for idx, file_path in enumerate(parquet_files, 1):
            print(f"\n[{idx}/{len(parquet_files)}] Traitement de {file_path.name}...", flush=True)
            sys.stdout.flush()
            
            # Import du fichier (0 si déjà importé)
            rows = self.import_parquet(file_path)
            total_rows += rows
            
            if rows > 0:
                print(f"     [OK] {rows:,} nouvelles lignes importées", flush=True)
            sys.stdout.flush()
        
        # ─────────────────────────────────────────────────────────────────
        # Étape 4: Afficher le résumé final
        # ─────────────────────────────────────────────────────────────────
        print(f"\n{'='*70}", flush=True)
        print(f"IMPORT TERMINÉ : {total_rows:,} lignes importées au total", flush=True)
        print(f"{'='*70}\n", flush=True)
        return total_rows
    
    # ═══════════════════════════════════════════════════════════════════════
    # STATISTIQUES ET VÉRIFICATIONS
    # ═══════════════════════════════════════════════════════════════════════
    
    def get_table_info(self):
        """
        Affiche les informations de base sur la table yellow_taxi_trips.
        
        UTILITÉ:
            Vérifier que l'import s'est bien passé.
            Voir rapidement le volume de données.
        
        EXEMPLE DE SORTIE:
            Nombre de lignes dans yellow_taxi_trips: 23,650,112
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                count = result.scalar()
                print(f"\nNombre de lignes dans yellow_taxi_trips: {count:,}")
        except Exception as e:
            print(f"[ERREUR] {e}")
    
    def get_statistics(self):
        """
        Affiche des statistiques détaillées sur les imports effectués.
        
        INFORMATIONS AFFICHÉES:
            - Nombre total de trajets dans yellow_taxi_trips
            - Nombre de fichiers Parquet importés
            - Date du premier import
            - Date du dernier import
            - Total de lignes importées (somme de tous les fichiers)
        
        SOURCE DES DONNÉES:
            - Table yellow_taxi_trips: Données NYC Taxi
            - Table import_log: Métadonnées d'import
        
        EXEMPLE DE SORTIE:
            Statistiques détaillées:
            ──────────────────────────────────────────────────
            Nombre total de trajets: 23,650,112
            Nombre de fichiers importés: 8
            Premier import: 2025-01-15 10:23:45
            Dernier import: 2025-01-15 10:45:12
            Total lignes importées: 23,650,112
        """
        print("\nStatistiques détaillées:")
        print("-" * 50)
        
        try:
            db: Session = SessionLocal()
            try:
                # ─────────────────────────────────────────────────────────
                # 1. Nombre total de trajets dans la table principale
                # ─────────────────────────────────────────────────────────
                with self.engine.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
                    count = result.scalar()
                    print(f"Nombre total de trajets: {count:,}")
                
                # ─────────────────────────────────────────────────────────
                # 2. Nombre de fichiers importés
                # ─────────────────────────────────────────────────────────
                count = db.query(ImportLog).count()
                print(f"Nombre de fichiers importés: {count}")
                
                # ─────────────────────────────────────────────────────────
                # 3. Plage de dates d'import (premier et dernier)
                # ─────────────────────────────────────────────────────────
                from sqlalchemy import func
                result = db.query(
                    func.min(ImportLog.import_date),
                    func.max(ImportLog.import_date)
                ).first()
                
                if result[0]:
                    print(f"Premier import: {result[0]}")
                    print(f"Dernier import: {result[1]}")
                
                # ─────────────────────────────────────────────────────────
                # 4. Total de lignes importées (somme de tous les fichiers)
                # ─────────────────────────────────────────────────────────
                total = db.query(func.sum(ImportLog.rows_imported)).scalar()
                if total:
                    print(f"Total lignes importées: {total:,}")
                    
            finally:
                db.close()
                
        except Exception as e:
            print(f"[ERREUR] Échec de la récupération des statistiques: {e}")
    
    def close(self):
        """
        Ferme proprement les connexions à PostgreSQL.
        
        POURQUOI C'EST IMPORTANT?
            - Libère les connexions dans le pool SQLAlchemy
            - Évite les "connexions zombies" (connexions ouvertes inutilement)
            - Bonne pratique: toujours fermer après usage
        
        QUAND APPELER?
            À la fin du script, après tous les imports.
            Ou dans un bloc finally pour garantir la fermeture même en cas d'erreur.
        
        EXEMPLE:
            try:
                importer = PostgreSQLImporter()
                importer.import_all_parquet_files("src/data/raw")
            finally:
                importer.close()  # Toujours exécuté
        """
        self.engine.dispose()
        print("\n[OK] Connexions PostgreSQL fermées proprement")


# ═══════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE PRINCIPAL (exécution en tant que script)
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Exécution du script en mode standalone.
    
    USAGE:
        python src/import_to_postgres.py
    
    OU avec Docker:
        docker exec -it taxi_api python /app/src/import_to_postgres.py
    
    FLUX:
        1. Créer l'importer
        2. Importer tous les fichiers Parquet du répertoire src/data/raw
        3. Afficher les informations de la table
        4. Afficher les statistiques détaillées
        5. Fermer les connexions
    """
    print("\n" + "="*70)
    print("DÉMARRAGE DU SCRIPT D'IMPORT NYC TAXI → POSTGRESQL")
    print("="*70 + "\n")
    
    importer = PostgreSQLImporter()
    
    try:
        # Import de tous les fichiers Parquet
        importer.import_all_parquet_files(Path("src/data/raw"))
        
        # Affichage des informations
        importer.get_table_info()
        importer.get_statistics()
        
    except Exception as e:
        print(f"\n[ERREUR FATALE] {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Toujours fermer les connexions (même en cas d'erreur)
        importer.close()
    
    print("\n" + "="*70)
    print("SCRIPT TERMINÉ")
    print("="*70 + "\n")
