"""
Configuration de la connexion base de données avec SQLAlchemy.
Supporte PostgreSQL (Docker) et DuckDB (local) selon USE_POSTGRES.
"""
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

try:
    from config import config
except ImportError:
    # Fallback si config.py n'est pas dans le path
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from config import config


def get_database_url() -> str:
    """
    Construit l'URL de connexion selon USE_POSTGRES.
    - PostgreSQL si USE_POSTGRES=true (Docker)
    - DuckDB si USE_POSTGRES=false (local)
    
    Returns:
        URL de connexion au format SQLAlchemy
    """
    if config.USE_POSTGRES:
        # PostgreSQL (Docker)
        return f"postgresql://{config.POSTGRES_USER}:{config.POSTGRES_PASSWORD}@{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
    else:
        # DuckDB (local) - utilise la base existante
        db_path = Path(__file__).parent / "data" / "taxi_data.duckdb"
        return f"duckdb:///{db_path.absolute()}"


# Créer le moteur SQLAlchemy
DATABASE_URL = get_database_url()

if config.USE_POSTGRES:
    # PostgreSQL avec pool_pre_ping
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    print(f"Mode: PostgreSQL ({config.POSTGRES_HOST}:{config.POSTGRES_PORT})")
else:
    # DuckDB sans pool
    engine = create_engine(DATABASE_URL, connect_args={"read_only": False})
    print(f"Mode: DuckDB (local, read-only recommandé)")

# Créer la factory de sessions
# Pourquoi autocommit=False: transactions explicites (plus sûr).
# Pourquoi autoflush=False: flush manuel pour plus de contrôle.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base pour les modèles SQLAlchemy
Base = declarative_base()


def get_db():
    """
    Générateur de session pour FastAPI (dépendance).
    Garantit que la session est fermée après utilisation.
    
    Yields:
        Session SQLAlchemy
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """
    Initialise la base de données en créant toutes les tables.
    Idempotent: ne fait rien si les tables existent déjà.
    """
    Base.metadata.create_all(bind=engine)
    print("Tables créées (ou déjà existantes)")

