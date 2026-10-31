import argparse

import duckdb

from src.config import EXPORT_DIR, GOLD_DB_PATH, GOLD_TABLES


def export_table(con: duckdb.DuckDBPyConnection, table: str, out_dir) -> int:
    """Exporte une table Gold en Parquet et en CSV. Retourne le nombre de lignes."""
    n_rows = con.execute(f"select count(*) from {table}").fetchone()[0]
    con.execute(f"copy {table} to '{(out_dir / table).as_posix()}.parquet' (format parquet)")
    con.execute(f"copy {table} to '{(out_dir / table).as_posix()}.csv' (format csv, header)")
    return n_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(GOLD_DB_PATH))
    parser.add_argument("--out", default=str(EXPORT_DIR))
    args = parser.parse_args()

    from pathlib import Path

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(args.db, read_only=True)
    for table in GOLD_TABLES:
        n_rows = export_table(con, table, out_dir)
        print(f"{table}: {n_rows} lignes -> {out_dir}")
    con.close()


if __name__ == "__main__":
    main()