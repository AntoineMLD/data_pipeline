# File: Dockerfile
# Image de base légère avec Python 3.11
FROM python:3.11-slim

# Répertoire de travail à l'intérieur du conteneur
WORKDIR /app

# ---- Dépendances système ----
# gcc : utile pour compiler certains paquets
# postgresql-client : pratique pour diagnostiquer la DB (psql)
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# ---- Dépendances Python ----
# Copier requirements en amont pour bénéficier du cache Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Code de l'application ----
# On copie uniquement le dossier src (tes données sont sous src/data)
COPY src/ ./src/

# ---- Réseau / Exposition du service ----
EXPOSE 8000

# ---- Commande par défaut ----
# (docker-compose override aussi cette commande avec --reload)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
