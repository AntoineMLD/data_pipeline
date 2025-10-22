# Image Python officielle légère
FROM python:3.12-slim

# Variables d'environnement pour éviter les fichiers .pyc et buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Répertoire de travail dans le conteneur
WORKDIR /app

# Installer les dépendances système nécessaires pour psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copier les fichiers de dépendances
COPY requirements.txt pyproject.toml ./

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source
COPY src/ ./src/

# Copier le script d'entrypoint
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Créer le répertoire pour les données
RUN mkdir -p src/data/raw

# Installer postgresql-client pour pg_isready
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Exposer le port de l'API
EXPOSE 8000

# Point d'entrée
ENTRYPOINT ["/docker-entrypoint.sh"]

