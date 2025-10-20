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
```

## Notes

- Pour Docker : `POSTGRES_HOST=postgres`
- Pour local : `POSTGRES_HOST=localhost`
- Pour DuckDB local : `USE_POSTGRES=false`

