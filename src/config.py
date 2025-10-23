"""
Configuration centralisée de l'application.

Charge les variables d'environnement depuis un fichier `.env` à la racine
du projet (via python-dotenv) et expose un objet `config` simple à utiliser.

Variables supportées (voir `env.example.md` et `README.md`):
  - USE_POSTGRES        (bool)   : true pour PostgreSQL, false pour DuckDB
  - POSTGRES_HOST       (str)
  - POSTGRES_PORT       (int)
  - POSTGRES_DB         (str)
  - POSTGRES_USER       (str)
  - POSTGRES_PASSWORD   (str)

Exemple d'utilisation:
    from config import config
    if config.USE_POSTGRES:
        ...
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _parse_bool(value: str | None, default: bool) -> bool:
    """Convertit une chaîne en booléen.

    Accepte: "true", "1", "yes", "y", "on" (insensible à la casse).
    Tout autre valeur → False. None → default.
    """
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(value: str | None, default: int) -> int:
    """Convertit une chaîne en int avec valeur par défaut sûre."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class Config:
    """Paramètres applicatifs typés et simples à consommer."""

    USE_POSTGRES: bool = True
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "taxi_data"
    POSTGRES_USER: str = "pipeline_user"
    POSTGRES_PASSWORD: str = "pipeline_pass"

    @staticmethod
    def from_env() -> "Config":
        """Construit une configuration à partir des variables d'environnement."""
        # Charger le .env à la racine du projet si présent
        # On résout par rapport à ce fichier pour fonctionner depuis root ou src/
        project_root = Path(__file__).resolve().parents[1]
        load_dotenv(project_root / ".env")

        return Config(
            USE_POSTGRES=_parse_bool(os.getenv("USE_POSTGRES"), True),
            POSTGRES_HOST=os.getenv("POSTGRES_HOST", "localhost"),
            POSTGRES_PORT=_parse_int(os.getenv("POSTGRES_PORT"), 5432),
            POSTGRES_DB=os.getenv("POSTGRES_DB", "taxi_data"),
            POSTGRES_USER=os.getenv("POSTGRES_USER", "pipeline_user"),
            POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
        )


# Instance prête à l'emploi
config = Config.from_env()

