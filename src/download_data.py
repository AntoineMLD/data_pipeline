# File: src/download_data.py
from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import requests
from requests.exceptions import RequestException, HTTPError


class NYCTaxiDataDownloader:
    """
    Télécharge les Parquet Yellow Taxi pour une année.
    Pourquoi ce design:
      - Classe paramétrable (année, dossier, retries) → facilement testable/réutilisable.
      - Écriture atomique (.part → rename) → jamais de fichier final corrompu.
      - HEAD avant GET → distingue "mois non publié" d'un incident réseau.
    """

    # CDN officiel TLC: stable et rapide; éviter de scraper des pages HTML.
    BASE_URL: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    def __init__(
        self,
        year: int = 2025,
        data_dir: Path | str = Path("src/data/raw"),
        timeout: int = 30,
        chunk_size: int = 8192,
        max_retries: int = 3,
        backoff_base: float = 1.5,
        progress: bool = True,
    ) -> None:
        # Pourquoi stocker en attributs: injection de dépendances (tests) et flexibilité d’usage.
        self.YEAR: int = year
        self.DATA_DIR: Path = Path(data_dir)
        self.TIMEOUT: int = timeout            # borne max d’attente réseau par requête
        self.CHUNK_SIZE: int = chunk_size      # taille des morceaux pour limiter la RAM
        self.MAX_RETRIES: int = max_retries    # robustesse face aux pannes transitoires
        self.BACKOFF_BASE: float = backoff_base  # exponentiel doux (1.5^n)
        self.SHOW_PROGRESS: bool = progress    # affichage console non bloquant

        # Idempotent: crée l’arborescence si absente (pas d’échec si déjà là).
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ---------- helpers ----------
    def get_file_path(self, month: int) -> Path:
        """Construit le chemin cible unique (source de vérité du nommage)."""
        self._validate_month(month)
        return self.DATA_DIR / f"yellow_tripdata_{self.YEAR}-{month:02d}.parquet"

    def file_exists(self, month: int) -> bool:
        """Évite les re-téléchargements inutiles (gain de temps/bande passante)."""
        return self.get_file_path(month).is_file()

    def _build_url(self, month: int) -> str:
        # Même convention nom/URL → logique et tests simplifiés.
        self._validate_month(month)
        return f"{self.BASE_URL}/yellow_tripdata_{self.YEAR}-{month:02d}.parquet"

    @staticmethod
    def _validate_month(month: int) -> None:
        # Fail fast: erreur claire plutôt que comportement implicite.
        if not 1 <= month <= 12:
            raise ValueError(f"month must be in 1..12, got {month}")

    # ---------- existence check (HEAD) ----------
    def _remote_exists(self, month: int) -> bool:
        """
        Vérifie l’existence côté serveur via HEAD.
        Pourquoi: ne pas confondre un 404 (mois non publié) avec un problème réseau.
        """
        url = self._build_url(month)
        try:
            status = self._head_with_retries(url)
            if 200 <= status < 400:
                return True     # existe (OK/redirection)
            if status == 404:
                return False    # non publié → on n'insiste pas
            return True         # codes atypiques: on laissera le GET décider avec retries
        except RequestException:
            return True         # en cas de panne HEAD, on tente quand même le GET

    def _head_with_retries(self, url: str) -> int:
        """
        HEAD robuste avec backoff exponentiel.
        Pourquoi: CloudFront répond 404 rapidement si l'objet n'existe pas.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = requests.head(url, timeout=self.TIMEOUT, allow_redirects=True)
                return resp.status_code
            except RequestException as e:
                last_exc = e
                if attempt < self.MAX_RETRIES:
                    backoff = self.BACKOFF_BASE ** (attempt - 1)
                    print(f"[warn] HEAD tentative {attempt}/{self.MAX_RETRIES} échouée: {e}. Retry dans {backoff:.1f}s")
                    time.sleep(backoff)  # Pourquoi: laisser le réseau “respirer”
                else:
                    print(f"[error] HEAD tentative {attempt}/{self.MAX_RETRIES} échouée: {e}.")
        assert last_exc is not None
        # Propager l'erreur standardisée pour traitement amont.
        raise RequestException(str(last_exc))

    # ---------- core ----------
    def download_month(self, month: int) -> bool:
        """
        Télécharge un mois.
        True si le fichier est présent (déjà là ou téléchargé), False en cas d'échec réseau final.
        """
        dest: Path = self.get_file_path(month)
        if dest.exists():
            print(f"[skip] {dest.name} déjà présent")
            return True

        url = self._build_url(month)
        # .part = écriture temporaire pour garantir que seul un fichier complet apparaît.
        tmp_dest = dest.with_suffix(dest.suffix + ".part")

        print(f"[info] Téléchargement: {url}")
        ok = self._download_with_retries(url, tmp_dest)
        if not ok:
            # Nettoyer les traces d’un téléchargement interrompu → pas d’état incohérent.
            if tmp_dest.exists():
                try:
                    tmp_dest.unlink()
                except Exception:
                    pass
            print(f"[fail] Échec: {dest.name}")
            return False

        # Rename atomique: publication du fichier “complet” d’un coup.
        tmp_dest.replace(dest)
        print(f"[ok] {dest.name} enregistré ({dest.stat().st_size:,} bytes)")
        return True

    def download_all_available(self) -> List[Path]:
        """
        Pilote l’ensemble de l’année:
          - jusqu’au mois courant si YEAR == année courante, sinon 12.
          - marque explicitement les mois non publiés (missing) sans les compter comme échecs.
        """
        today = datetime.now()
        end_month = today.month if self.YEAR == today.year else 12

        results: List[Path] = []
        successes = 0
        skips = 0
        failures: list[int] = []
        missing: list[int] = []

        print(
            f"[start] Année {self.YEAR} | Mois 1..{end_month} → dossier: {self.DATA_DIR.resolve()}"
        )
        for m in range(1, end_month + 1):
            dest = self.get_file_path(m)

            if dest.exists():
                print(f"[skip] {dest.name} déjà présent")
                results.append(dest)
                skips += 1
                continue

            # HEAD avant GET → évite des retries inutiles sur un 404 “normal”.
            if not self._remote_exists(m):
                print(f"[missing] {dest.name} non publié (404)")
                missing.append(m)
                continue

            # Téléchargement résilient si le fichier est annoncé comme existant.
            if self.download_month(m):
                results.append(dest)
                successes += 1
            else:
                failures.append(m)

        # Résumé exploitable (observabilité): permet de décider quoi rejouer.
        print("\n[summary]")
        print(f"  Nouveaux téléchargements : {successes}")
        print(f"  Déjà présents            : {skips}")
        print(f"  Manquants (non publiés)  : {len(missing)}")
        if missing:
            print(f"    Mois manquants         : {', '.join(f'{x:02d}' for x in missing)}")
        print(f"  Échecs réseau            : {len(failures)}")
        if failures:
            print(f"    Mois en échec          : {', '.join(f'{x:02d}' for x in failures)}")

        return results

    # ---------- network ----------
    def _download_with_retries(self, url: str, dest_tmp: Path) -> bool:
        """
        GET en streaming avec retries + backoff.
        Pourquoi:
          - streaming par chunks → faible empreinte mémoire.
          - retries → résilience aux micro-coupures/throttling.
          - .part → pas de publication d’un fichier tronqué.
        """
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                with requests.get(url, stream=True, timeout=self.TIMEOUT) as resp:
                    try:
                        resp.raise_for_status()
                    except HTTPError as he:
                        # 404 attendu si HEAD a raté: ne pas insister.
                        if he.response is not None and he.response.status_code == 404:
                            return False
                        raise  # autres codes → laisser la boucle de retries gérer

                    total = int(resp.headers.get("Content-Length", "0")) or None

                    # Évite de mixer des restes d’un ancien essai.
                    if dest_tmp.exists():
                        try:
                            dest_tmp.unlink()
                        except Exception:
                            pass

                    bytes_done = 0
                    t0 = time.time()
                    with open(dest_tmp, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=self.CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            # Progression non bloquante en stderr (lisible en CI).
                            if self.SHOW_PROGRESS:
                                bytes_done += len(chunk)
                                self._print_progress(bytes_done, total, t0)
                    if self.SHOW_PROGRESS:
                        sys.stderr.write("\n")

                    # Sanity check: refuser le fichier vide.
                    if dest_tmp.stat().st_size > 0:
                        return True
                    else:
                        raise RequestException("Réponse vide (0 bytes)")

            except RequestException as e:
                # Nettoyage pour éviter un état partiel entre tentatives.
                if dest_tmp.exists():
                    try:
                        dest_tmp.unlink()
                    except Exception:
                        pass
                if attempt < self.MAX_RETRIES:
                    backoff = self.BACKOFF_BASE ** (attempt - 1)
                    print(f"[warn] Tentative {attempt}/{self.MAX_RETRIES} échouée: {e}. Retry dans {backoff:.1f}s")
                    time.sleep(backoff)
                else:
                    print(f"[error] Tentative {attempt}/{self.MAX_RETRIES} échouée: {e}. Abandon.")
                    return False
            except Exception as e:
                # Garde-fou contre toute erreur inattendue → laisse l’appelant décider.
                if dest_tmp.exists():
                    try:
                        dest_tmp.unlink()
                    except Exception:
                        pass
                print(f"[error] Erreur inattendue: {e}")
                return False

        return False

    @staticmethod
    def _format_size(num_bytes: int) -> str:
        # Lisibilité humaine des tailles → diagnostic plus intuitif.
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(num_bytes)
        for u in units:
            if size < 1024.0 or u == units[-1]:
                return f"{size:,.1f} {u}"
            size /= 1024.0
        return f"{num_bytes} B"

    def _print_progress(self, bytes_done: int, total: Optional[int], t0: float) -> None:
        """Affiche une barre simple; en stderr pour ne pas polluer la stdout (scripts)."""
        elapsed = max(time.time() - t0, 1e-6)
        speed = bytes_done / elapsed
        if total:
            pct = bytes_done / total * 100
            msg = (
                f"\r[dl] {pct:6.2f}% | "
                f"{self._format_size(bytes_done)} / {self._format_size(total)} | "
                f"{self._format_size(speed)}/s"
            )
        else:
            msg = f"\r[dl] {self._format_size(bytes_done)} | {self._format_size(speed)}/s"
        sys.stderr.write(msg)
        sys.stderr.flush()


def _ensure_project_layout() -> None:
    """
    Garantit la structure du projet.
    Pourquoi: éviter des erreurs “No such file or directory” lors des premiers runs/CI.
    """
    Path("src/data").mkdir(parents=True, exist_ok=True)
    Path("src/data/raw").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    # Permet un usage direct (script) et modulaire (import/tests).
    _ensure_project_layout()
    downloader = NYCTaxiDataDownloader(
        year=2025,
        data_dir=Path("src/data/raw"),
        timeout=30,
        chunk_size=8192,
        max_retries=3,
        backoff_base=1.6,   # léger ajustement du backoff par défaut
        progress=True,
    )
    downloader.download_all_available()
