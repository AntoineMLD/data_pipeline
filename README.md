# Data Pipeline NYC Taxi

Pipeline de données pour télécharger et importer les données NYC Yellow Taxi depuis le CDN officiel.

## Architecture

- **Téléchargement** : `src/download_data.py` - Téléchargement robuste avec retries et gestion des erreurs
- **Database** : `src/database.py` - Configuration SQLAlchemy (engine, session, models)
- **Import DuckDB** : `src/import_to_duckdb.py` - Import local avec suivi des fichiers
- **Import PostgreSQL** : `src/import_to_postgres.py` - Import en base de données avec SQLAlchemy
- **Main** : `src/main.py` - Orchestration avec choix DuckDB/PostgreSQL

## Démarrage rapide

### Configuration initiale

1. Créer le fichier `.env` à la racine du projet (voir `env.example.md`)

```bash
# Copier le contenu depuis env.example.md
cp env.example.md .env
# Puis éditer .env avec vos valeurs
```

### Avec Docker Compose (PostgreSQL)

```bash
# Construire et lancer les services
docker-compose up --build

# Lancer uniquement PostgreSQL
docker-compose up postgres

# Nettoyer
docker-compose down -v
```

### Local (DuckDB)

```bash
# Installer les dépendances
pip install -r requirements.txt

# Créer le fichier .env et mettre USE_POSTGRES=false

# Télécharger les données
python src/download_data.py

# Importer dans DuckDB
python src/main.py
```

## Configuration

Variables d'environnement (voir `env.example.md`) :

Le projet utilise `python-dotenv` pour charger les variables depuis un fichier `.env`.

- `USE_POSTGRES` : `true` pour PostgreSQL, `false` pour DuckDB (défaut: `true`)
- `POSTGRES_HOST` : Adresse du serveur PostgreSQL (défaut: `localhost`)
- `POSTGRES_PORT` : Port PostgreSQL (défaut: `5432`)
- `POSTGRES_DB` : Nom de la base (défaut: `taxi_data`)
- `POSTGRES_USER` : Utilisateur (défaut: `pipeline_user`)
- `POSTGRES_PASSWORD` : Mot de passe (défaut: `pipeline_pass`)

**Important** : Le fichier `.env` n'est pas versionné (dans `.gitignore`). Utilisez `env.example.md` comme référence.

## Workflow CI/CD

Le workflow GitHub Actions (`.github/workflows/docker-build.yml`) :

- Build automatique sur push vers `main` ou `docker_postgres_A`
- Publication vers GitHub Container Registry (ghcr.io)
- Cache des layers Docker pour accélérer les builds
- Tagging automatique (branches, PRs, semver, commit SHA)

Pour publier une version :

```bash
git tag v1.0.0
git push origin v1.0.0
```

## Structure du projet

```
.
├── src/
│   ├── data/
│   │   └── raw/           # Fichiers parquet téléchargés
│   ├── database.py        # Configuration SQLAlchemy
│   ├── download_data.py   # Téléchargeur NYC Taxi
│   ├── import_to_duckdb.py
│   ├── import_to_postgres.py
│   └── main.py
├── .github/
│   └── workflows/
│       └── docker-build.yml
├── docker-compose.yml
├── Dockerfile
├── .dockerignore
└── requirements.txt
```

## Principes de code

Ce projet respecte les principes du **clean code** :

- Responsabilités bien séparées (une classe = une responsabilité)
- Pas de logique cachée ou implicite
- Fonctions courtes et bien nommées
- Docstrings et commentaires explicatifs (pourquoi, pas comment)
- Gestion d'erreurs robuste avec retries
- Code simple, lisible et maintenable
