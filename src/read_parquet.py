# File: src/read_parquet.py
from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Pourquoi: chemin unique "source de vérité" → évite les concat de chaînes fragiles.
RAW_DIR = Path("src/data/raw")
# Pourquoi: valeur par défaut raisonnable pour une inspection rapide en console.
DEFAULT_ROWS = 5


def list_parquet_files() -> List[Path]:
    """
    Liste triée des fichiers Parquet présents.
    Pourquoi:
      - tri → sortie déterministe (utile pour comparer des exécutions).
      - fail fast si dossier manquant → message d'erreur clair.
    """
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Dossier introuvable: {RAW_DIR.resolve()}")
    return sorted(RAW_DIR.glob("*.parquet"))


def head_parquet(path: Path, n: int) -> pd.DataFrame:
    """
    Retourne efficacement les n premières lignes d'un Parquet.
    Pourquoi:
      - lecteur par *batches* (iter_batches) → pas de lecture complète, faible RAM.
      - compatible avec versions PyArrow où Dataset.to_table(limit=...) n'existe pas.
      - tronque précisément à n lignes même si le dernier batch dépasse.
    """
    pf = pq.ParquetFile(path)

    batches = []
    total = 0
    # batch_size=n pour limiter la quantité lue; on arrête dès qu'on atteint n lignes
    for batch in pf.iter_batches(batch_size=n):
        batches.append(batch)
        total += batch.num_rows
        if total >= n:
            break

    if not batches:
        # Pourquoi: fichier vide → retourner un DF vide avec le bon schéma (colonnes d'attendu).
        schema = pf.schema_arrow
        empty_tbl = pa.Table.from_arrays(
            [pa.array([], type=f.type) for f in schema],
            names=[f.name for f in schema],
        )
        return empty_tbl.to_pandas(types_mapper=pd.ArrowDtype)

    table = pa.Table.from_batches(batches)
    # Pourquoi: peut dépasser n si le dernier batch est plus gros → re-slice.
    if table.num_rows > n:
        table = table.slice(0, n)

    # Pourquoi: ArrowDtype garde des types plus stables en pandas 2.x.
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def print_heads(n: int) -> None:
    """
    Affiche les n premières lignes pour chaque fichier.
    Pourquoi:
      - boucle tolérante aux erreurs fichier par fichier (continue en cas d'échec).
      - formattage lisible pour une revue console.
    """
    files = list_parquet_files()
    if not files:
        print(f"[info] Aucun fichier .parquet trouvé dans {RAW_DIR.resolve()}")
        return

    print(
        f"[start] {len(files)} fichier(s) trouvé(s) dans {RAW_DIR.resolve()} — "
        f"affichage des {n} premières lignes\n"
    )
    for i, path in enumerate(files, 1):
        try:
            df = head_parquet(path, n)
            print(f"=== ({i}/{len(files)}) {path.name} ===")
            # Pourquoi: options d'affichage locales pour ne pas tronquer visuellement.
            with pd.option_context("display.max_columns", None, "display.width", 160):
                print(df)
        except Exception as e:
            # Pourquoi: diagnostiquer sans stopper la boucle (diagnostic granulaire).
            print(f"[error] {path.name}: {e}")
        print()  # séparation visuelle entre fichiers


def parse_args() -> int:
    """
    Parse les arguments CLI.
    Pourquoi:
      - --rows configurable sans modifier le code.
      - validation minimale pour éviter des appels incohérents.
    """
    parser = ArgumentParser(
        description="Afficher les premières lignes de chaque Parquet dans src/data/raw"
    )
    parser.add_argument(
        "--rows",
        "-n",
        type=int,
        default=DEFAULT_ROWS,
        help="Nombre de lignes à afficher par fichier (défaut: 5)",
    )
    args = parser.parse_args()
    if args.rows <= 0:
        raise ValueError("--rows doit être > 0")
    return args.rows


if __name__ == "__main__":
    # Pourquoi: point d'entrée scriptable (uv run / python), réutilisable en import.
    n = parse_args()
    print_heads(n)
