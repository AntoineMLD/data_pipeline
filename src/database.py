# File: src/database.py
"""
Configuration centralisée de la base PostgreSQL (SQLAlchemy 2.x).
Objectifs pédagogiques :
  - Rassembler la config de connexion DB (URL, engine, sessions).
  - Fournir une dépendance FastAPI standard (get_db()).
  - Offrir une fonction d'initialisation (init_db) pour créer les tables.
"""

from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session


# =========================
# 1) Lecture de la configuration depuis l'environnement
# =========================
# Pourquoi : Docker Compose injecte ces variables (voir .env / docker-compose.yml).
#            On met des valeurs par défaut compatibles avec ton .env.example
#            pour un démarrage "out-of-the-box" en local.

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_taxi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")  # nom du service docker
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Construction explicite de l'URL DSN PostgreSQL.
# IMPORTANT : si ton mot de passe contient des caractères spéciaux, pense à l'encoder.
DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


# =========================
# 2) Création du moteur (Engine) et de la fabrique de sessions
# =========================
# create_engine : point d'entrée bas niveau de SQLAlchemy pour gérer les connexions.
# - pool_pre_ping=True : vérifie la validité de la connexion avant usage (évite les 'server closed the connection').
# - echo=False : désactive le log SQL verbeux (peut être activé en dev si besoin).
# - future=True : style 2.0 (déjà par défaut en 2.x, mais explicite aide à la lecture).
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    echo=False,
    future=True,
)

# sessionmaker : crée une "fabrique" de Session configurée.
# - autocommit=False : on contrôle explicitement les transactions via commit/rollback.
# - autoflush=False : évite des flush implicites surprenants ; on flush/commit quand on le décide.
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
)

# Base déclarative : toutes tes classes de modèles SQLAlchemy devront hériter de `Base`.
# Exemple (à mettre ailleurs) :
#   class MyModel(Base):
#       __tablename__ = "my_table"
#       id = mapped_column(Integer, primary_key=True)
Base = declarative_base()


# =========================
# 3) Dépendance FastAPI : get_db()
# =========================
# Pattern recommandé :
#   - Ouvre une session par requête HTTP.
#   - La fournit aux endpoints via `Depends(get_db)`.
#   - Ferme la session proprement (finally), quel que soit le résultat.
def get_db() -> Generator[Session, None, None]:
    """
    Générateur de session pour FastAPI (ou usage script).
    Usage :
        @app.get("/items")
        def list_items(db: Session = Depends(get_db)):
            return db.execute(select(...)).all()
    """
    db = SessionLocal()
    try:
        yield db  # la session est utilisable ici (dans la vue FastAPI)
    finally:
        # Toujours fermer pour libérer la connexion au pool.
        db.close()


# =========================
# 4) Initialisation du schéma
# =========================
# Crée les tables définies par *tous* les modèles qui héritent de Base.
# ATTENTION :
#   - Assure-toi que tes modules de modèles sont importés AVANT d'appeler init_db(),
#     pour que leurs classes soient enregistrées dans Base.metadata.
def init_db() -> None:
    """
    Crée les tables en base selon les modèles déclarés.
    Appeler une fois au démarrage (ou via une commande de migration).
    """
    # Exemple si tes modèles sont dans src/models.py :
    # from src import models  # noqa: F401  # important: charge les classes dans Base.metadata
    Base.metadata.create_all(bind=engine)
