"""
Logique métier pour l'API FastAPI (services).

Les méthodes opèrent sur la table `yellow_taxi_trips` via SQLAlchemy.
Comme le schéma peut évoluer, on garde des opérations basiques autour de l'ID.
"""

from __future__ import annotations

from typing import List, Tuple, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from models import YellowTaxiTrip


class TaxiTripService:
    @staticmethod
    def get_trip(db: Session, trip_id: int) -> Optional[Dict[str, Any]]:
        row = db.execute(text("SELECT * FROM yellow_taxi_trips WHERE index = :id LIMIT 1"), {"id": trip_id}).mappings().first()
        return dict(row) if row else None

    @staticmethod
    def get_trips(db: Session, skip: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        total = db.query(func.count(YellowTaxiTrip.index)).scalar() or 0
        rows = db.execute(
            text("""
                SELECT * FROM yellow_taxi_trips
                ORDER BY index ASC
                OFFSET :skip LIMIT :limit
            """),
            {"skip": skip, "limit": limit},
        ).mappings().all()
        trips = [dict(r) for r in rows]
        return trips, total

    @staticmethod
    def create_trip(db: Session) -> Dict[str, Any]:
        # Sans schéma strict, on crée une ligne vide (si autorisé par la table)
        db.execute(text("INSERT INTO yellow_taxi_trips DEFAULT VALUES"))
        db.commit()
        row = db.execute(text("SELECT * FROM yellow_taxi_trips ORDER BY index DESC LIMIT 1")).mappings().first()
        return dict(row) if row else {"index": None}

    @staticmethod
    def update_trip(db: Session, trip_id: int) -> Optional[Dict[str, Any]]:
        obj = TaxiTripService.get_trip(db, trip_id)
        if not obj:
            return None
        # Pas de champs à updater tant que le schéma n'est pas figé.
        return obj

    @staticmethod
    def delete_trip(db: Session, trip_id: int) -> bool:
        result = db.execute(text("DELETE FROM yellow_taxi_trips WHERE index = :id"), {"id": trip_id})
        db.commit()
        return result.rowcount > 0

    @staticmethod
    def get_statistics(db: Session):
        total = db.query(func.count(YellowTaxiTrip.index)).scalar() or 0

        # Dates d'import issues d'import_log  → requête SQL
        result = db.execute(text("SELECT MIN(import_date), MAX(import_date) FROM import_log")).fetchone()
        first_import = result[0] if result and len(result) > 0 else None
        last_import = result[1] if result and len(result) > 1 else None

        # Exemple de statistique: moyenne d'un champ (fare_amount)
        average_fare = None
        try:
            res = db.execute(text("SELECT AVG(fare_amount) FROM yellow_taxi_trips")).fetchone()
            if res:
                average_fare = float(res[0]) if res[0] is not None else None
        except Exception:
            average_fare = None

        return {
            "total_trips": int(total),
            "first_import": first_import,
            "last_import": last_import,
            "average_fare": average_fare,
        }

