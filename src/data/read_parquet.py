from pyarrow.parquet import read_table
import pyarrow as pa

def read_pyarrow_parquet(file_path: str) -> pa.Table:
    """Lire un fichier Parquet avec PyArrow
    Retourne un objet Table PyArrow"""
    return read_table(file_path)

if __name__ == "__main__":
    file_path = "src/data/data/yellow_tripdata_2025-05.parquet"
    table = read_pyarrow_parquet(file_path)
    print(len(table))
