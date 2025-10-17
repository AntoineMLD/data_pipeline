from pathlib import Path
from typing import Optional, List
from datetime import datetime
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



class NYCTaxiDataDownloader:
    """Télécharge les données NYC Taxi au format Parquet.
    
    Cette classe gère le téléchargement des fichiers de données
    NYC Taxi depuis une URL source vers un répertoire local.
    Elle évite les téléchargements en double et gère les erreurs.
    
    Attributes:
        base_url (str): URL de base pour télécharger les fichiers
        year (int): Année des données à télécharger
        data_dir (str): Répertoire de destination des fichiers
    
    Example:
        >>> downloader = NYCTaxiDataDownloader(
        ...     base_url="https://example.com/data",
        ...     year=2025,
        ...     data_dir="./data"
        ... )
        >>> downloader.download_month(1)
    """
    
    def __init__(self, base_url: str, year: int, data_dir: str):
        """Initialise le téléchargeur de données NYC Taxi.
        
        Args:
            base_url: URL de base où se trouvent les fichiers Parquet
            year: Année des données à télécharger (ex: 2025)
            data_dir: Chemin du répertoire où sauvegarder les fichiers
        """
        self.base_url = base_url
        self.year = year
        self.data_dir = data_dir

    def get_file_path(self, month: int) -> Path:
        """ Construit le chemin du fichier pour un mois donné
        Format : yellow_tripdata_YYYY-MM.parquet"""
        return Path(self.data_dir) / f"yellow_tripdata_{self.year}-{month:02d}.parquet"

    def file_exists(self, month: int) -> bool:
        """ Vérifie si le fichier existe déjà localement
        Retourne True/False"""
        return self.get_file_path(month).exists()

    def download_month(self, month: int) -> bool:
        """ Vérifie si le fichier existe. 
        Affiche un message et retourne True si le fichier existe.
        Sinon, télécharge le fichier depuis la base_url.
        Utilise requests.get() avec stream=True.
        Affiche une barre de progression (optionnel).
        Gère les erreurs avec try/except.
        En cas d'erreur, supprime le fichier partiel.
        Retourne True si le fichier a été téléchargé avec succès, False sinon.
        """
        if self.file_exists(month):
            print(f"Le fichier {self.get_file_path(month)} existe déjà")
            return True
        
        # Créer le répertoire si nécessaire
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        
        file_path = self.get_file_path(month)
        try:
            response = requests.get(
                f"{self.base_url}/yellow_tripdata_{self.year}-{month:02d}.parquet", 
                stream=True, 
                timeout=30
            )
            response.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Fichier {file_path} téléchargé avec succès")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors du téléchargement du fichier {file_path}: {e}")
            # Supprimer le fichier partiel en cas d'erreur
            if file_path.exists():
                file_path.unlink()
            return False
    
    def download_all_available(self) -> list:
        """ Détermine le mois actuel.
        Boucle de janvier au mois actuel (si année 2025).
        Appel download-month() pour chaque mois.
        Retourne la liste des fichiers téléchargés.
        Affiche un résumé."""
        current_month = datetime.now().month
        current_year = datetime.now().year
        files_downloaded = []
        for month in range(1, current_month + 1):
            if self.download_month(month):
                files_downloaded.append(self.get_file_path(month))
        print(f"Fichiers téléchargés: {files_downloaded}")
        return files_downloaded
    
    
if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader(
        base_url="https://d37ci6vzurychx.cloudfront.net/trip-data",
        year=2025,
        data_dir="src/data/data"
    )
    downloader.download_all_available()

# les liens des fichiers parquet sur la page web
#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet
#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet