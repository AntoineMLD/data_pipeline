"""
Modèles SQLAlchemy pour l'API FastAPI.

On réutilise la table existante "yellow_taxi_trips" en PostgreSQL.
Comme le schéma peut évoluer, on déclare a minima une clé primaire `id` et
on laisse SQL côté table porter les autres colonnes (accès via requêtes brutes si besoin).

`ImportLog` est redéclaré ici pour l'ORM FastAPI, aligné avec la version utilisée
par l'importer PostgreSQL (même nom de table/colonnes).
"""

from __future__ import annotations

from sqlalchemy import Column, Integer, DateTime, Text

from src.database import Base


class YellowTaxiTrip(Base):
    __tablename__ = "yellow_taxi_trips"

    # Déclaration d'une PK logique côté ORM (la table n'a pas de PK SQL explicite)
    index = Column(Integer, primary_key=True, autoincrement=True)


class ImportLog(Base):
    __tablename__ = "import_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(Text)
    import_date = Column(DateTime)
    rows_imported = Column(Integer)

