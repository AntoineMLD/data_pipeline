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

# Lancer le nettoyage des données vers MongoDB en streaming
echo "Nettoyage des données (streaming) et sauvegarde MongoDB..."
python -u - <<'PY'
import os, sys, time
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/src')

CHUNK = int(os.getenv('CLEANER_CHUNK_SIZE', '100000'))

try:
    from src.data_cleaner import DataCleaner

    t0 = time.perf_counter()
    print(f"[CLEANER] Initialisation (chunksize={CHUNK})...", flush=True)
    cleaner = DataCleaner()

    try:
        t_stream = time.perf_counter()
        print("[CLEANER] Nettoyage en flux et insertion dans MongoDB (remplacement total)...", flush=True)
        totals = cleaner.stream_clean_to_mongodb(chunksize=CHUNK)
        t_end = time.perf_counter()

        print(
            "[CLEANER] Récap:",
            {
                'chunks': totals.get('chunks', 0),
                'lignes_chargees': totals.get('lignes_chargees', 0),
                'lignes_conservees': totals.get('lignes_conservees', 0),
                'documents_inserts': totals.get('documents_inserts', 0),
                'duree_s': round(t_end - t0, 3),
            },
            flush=True,
        )

    finally:
        cleaner.close()
        print("[CLEANER] Connexions fermées.", flush=True)

except Exception as e:
    print(f"[CLEANER][ERREUR] {e}", flush=True)
    print("[CLEANER] On continue le démarrage de l'API.", flush=True)
PY

# Lancer l'API
echo "Démarrage de l'API FastAPI..."
cd /app/src/fastapi-crud
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload

