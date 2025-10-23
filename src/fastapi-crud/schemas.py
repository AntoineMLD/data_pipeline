"""
Schémas Pydantic pour l'API.

Le schéma `TaxiTripBase` est généré dynamiquement à partir de la table
`yellow_taxi_trips` existante pour ne rien inventer et rester aligné avec
les vraies données.
"""

from __future__ import annotations

from typing import Optional, List, Any, Dict, Tuple, Type
from datetime import datetime
from pydantic import BaseModel, ConfigDict, create_model

from sqlalchemy import inspect as sa_inspect, Integer, Float, String, Text, DateTime, Boolean, BigInteger, Numeric
from src.database import engine


def _map_sqla_type_to_python(col_type: Any) -> Type[Any]:
    """Mappe un type SQLAlchemy vers un type Python basique pour Pydantic."""
    if isinstance(col_type, (Integer, BigInteger)):
        return int
    if isinstance(col_type, (Float, Numeric)):
        return float
    if isinstance(col_type, (String, Text)):
        return str
    if isinstance(col_type, Boolean):
        return bool
    if isinstance(col_type, DateTime):
        return datetime
    # Fallback
    return str


def _generate_taxi_trip_base_fields() -> Tuple[Dict[str, Tuple[Type[Any], None]], Optional[str]]:
    """Construit dynamiquement les champs du modèle à partir de la table.

    Returns:
        - dict des champs pour create_model
        - nom de la colonne PK (pour exclusion du schéma de base)
    """
    inspector = sa_inspect(engine)
    columns = inspector.get_columns("yellow_taxi_trips")
    pk_info = inspector.get_pk_constraint("yellow_taxi_trips") or {}
    pk_cols = pk_info.get("constrained_columns") or []
    pk_name = pk_cols[0] if pk_cols else None

    fields: Dict[str, Tuple[Type[Any], None]] = {}
    for col in columns:
        name = col.get("name")
        if name == pk_name:
            continue  # on n'inclut pas la PK dans le schéma de base
        py_type = _map_sqla_type_to_python(col.get("type"))
        fields[name] = (Optional[py_type], None)

    return fields, pk_name


# Génère le modèle TaxiTripBase au chargement du module
_fields, _pk_name = _generate_taxi_trip_base_fields()
TaxiTripBase = create_model("TaxiTripBase", **_fields)  # type: ignore[var-annotated]


class TaxiTripCreate(TaxiTripBase):
    pass


class TaxiTripUpdate(TaxiTripBase):
    pass


class TaxiTrip(TaxiTripBase):
    index: int


class TaxiTripList(BaseModel):
    total: int
    trips: List[TaxiTrip]


class Statistics(BaseModel):
    total_trips: int
    first_import: Optional[datetime] = None
    last_import: Optional[datetime] = None
    average_fare: Optional[float] = None


class PipelineResponse(BaseModel):
    success: bool
    message: str

