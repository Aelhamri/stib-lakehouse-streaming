"""Crée un petit échantillon du Silver pour la CI (lignes entières, 2 jours)."""
import shutil
from pathlib import Path

import duckdb

SOURCE = "data/lakehouse/silver/positions_tracked/*/*.parquet"
TARGET = Path("tests/fixtures/silver/positions_tracked")
LINES = ("73", "17")
DAYS = ("2026-05-05", "2026-05-06")

if TARGET.exists():
    shutil.rmtree(TARGET)
TARGET.mkdir(parents=True)

duckdb.sql(f"""
    copy (
        select *
        from read_parquet('{SOURCE}', hive_partitioning = true)
        where lineid in {LINES}
          and service_date in {DAYS}
    )
    to '{TARGET.as_posix()}'
    (format parquet, partition_by (service_date), compression zstd)
""")

size_kb = sum(f.stat().st_size for f in TARGET.rglob("*.parquet")) / 1024
print(f"Échantillon écrit dans {TARGET} : {size_kb:.0f} Ko")