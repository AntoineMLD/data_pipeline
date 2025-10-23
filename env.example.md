# Configuration .env

Copier ce contenu dans un fichier `.env` à la racine du projet :

```env
# Base de données à utiliser (true = PostgreSQL, false = DuckDB)
USE_POSTGRES=true

# Configuration PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=taxi_data
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=pipeline_pass

# MongoDB
MONGO_USER=admin
MONGO_PASSWORD=admin
MONGO_DB=nyc_taxi_clean
MONGO_HOST=mongodb
MONGO_PORT=27017

# Nettoyage en streaming (taille des chunks pour le DataCleaner)
CLEANER_CHUNK_SIZE=100000
```

## Notes

- Pour Docker : `POSTGRES_HOST=postgres`
- Pour local : `POSTGRES_HOST=localhost`
- Pour DuckDB local : `USE_POSTGRES=false`

