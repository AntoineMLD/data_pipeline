from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.database import engine, Base
from routes import router


# Créer les tables si besoin (import_log seulement; trips existe déjà)
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="API REST pour consulter et piloter la pipeline NYC Yellow Taxi.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "NYC Taxi Data Pipeline API"}


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(router, prefix="/api/v1")

