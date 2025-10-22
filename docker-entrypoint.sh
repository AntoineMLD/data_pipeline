#!/bin/bash
set -e

echo "Démarrage du conteneur API..."

# Attendre que PostgreSQL soit prêt
echo "Attente de PostgreSQL..."
while ! pg_isready -h $POSTGRES_HOST -p 5432 -U $POSTGRES_USER > /dev/null 2>&1; do
    echo "PostgreSQL n'est pas encore prêt, attente..."
    sleep 2
done
echo "PostgreSQL est prêt !"

# Importer les fichiers parquet (skip si déjà présents)
echo "Vérification et import des données parquet..."
python -u -c "
import sys
sys.path.insert(0, '/app/src')
from pathlib import Path
from import_to_postgres import PostgreSQLImporter

try:
    importer = PostgreSQLImporter()
    imported = importer.import_all_parquet_files(Path('/app/src/data/raw'))
    importer.close()
    print('Import terminé', flush=True)
except Exception as e:
    print(f'Erreur lors de l\'import: {e}', flush=True)
    print('L\'API démarrera quand même', flush=True)
"

# Lancer l'API
echo "Démarrage de l'API FastAPI..."
cd /app/src/fastapi-crud
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload

