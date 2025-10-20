"""
Configuration de la connexion PostgreSQL avec SQLAlchemy.
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()


def get_database_url() -> str:
    """
    Construit l'URL de connexion PostgreSQL depuis les variables d'environnement.
    
    Returns:
        URL de connexion au format SQLAlchemy
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    database = os.getenv("POSTGRES_DB", "taxi_data")
    user = os.getenv("POSTGRES_USER", "pipeline_user")
    password = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


# Créer le moteur SQLAlchemy
# Pourquoi pool_pre_ping: vérifie que la connexion est valide avant de l'utiliser.
DATABASE_URL = get_database_url()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

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
    print("✅ Tables créées (ou déjà existantes)")

