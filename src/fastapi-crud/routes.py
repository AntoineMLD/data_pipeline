from __future__ import annotations

"""
Routes FastAPI (couche API)

Organisation simple et claire:
- Dépendances: injection de la session DB via Depends(get_db)
- Services: toute la logique métier est dans TaxiTripService
- Schémas: réponses typées via Pydantic (schemas.py)

Règles de base:
- 404 si la ressource n'existe pas
- Pagination avec skip/limit
- Idempotence côté pipeline: relancer ne duplique pas
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.database import get_db
from schemas import TaxiTripList, TaxiTrip, Statistics, PipelineResponse
from services import TaxiTripService

router = APIRouter()


@router.get("/trips", response_model=TaxiTripList, tags=["Trips"])
def get_trips(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Liste paginée des trajets.

    - skip: nombre d'éléments à ignorer (offset)
    - limit: taille de page (max conseillé: 1000)
    Retourne total et la liste des trajets.
    """
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return {"total": total, "trips": trips}


@router.get("/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    """Récupère un trajet par son identifiant (colonne `index`).

    - trip_id: identifiant primaire logique du trajet
    404 si introuvable.
    """
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip


@router.post("/trips", response_model=TaxiTrip, tags=["Trips"])
def create_trip(db: Session = Depends(get_db)):
    """Crée un trajet minimal (ligne vide si schéma libre).

    Note: tant que le schéma n'est pas figé, on insère une ligne par défaut.
    """
    return TaxiTripService.create_trip(db)


@router.put("/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def update_trip(trip_id: int, db: Session = Depends(get_db)):
    """Met à jour un trajet existant (placeholder tant que le schéma n'est pas figé)."""
    updated = TaxiTripService.update_trip(db, trip_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Trip not found")
    return updated


@router.delete("/trips/{trip_id}", tags=["Trips"])
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    """Supprime un trajet par identifiant.

    Retourne {"success": true} si suppression effectuée, 404 sinon.
    """
    ok = TaxiTripService.delete_trip(db, trip_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"success": True}


@router.get("/statistics", response_model=Statistics, tags=["Statistics"])
def get_statistics(db: Session = Depends(get_db)):
    """Retourne quelques statistiques utiles (count, bornes d'import, moyenne)."""
    return TaxiTripService.get_statistics(db)


@router.post("/pipeline/run", response_model=PipelineResponse, tags=["Pipeline"])
def run_pipeline(db: Session = Depends(get_db)):
    """Déclenche le téléchargement + import (idempotent).

    S'appuie sur PostgreSQLImporter: ne réimporte pas les fichiers déjà traits.
    """
    from pathlib import Path
    from src.import_to_postgres import PostgreSQLImporter

    try:
        importer = PostgreSQLImporter()
        count = importer.import_all_parquet_files(Path("/app/src/data/raw"))
        importer.close()
        return PipelineResponse(success=True, message=f"Import ok, +{count} lignes")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")

