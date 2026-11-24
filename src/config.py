from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Ancien dossier (Parquet) : conservé comme référence, plus utilisé par les jobs.
LAKEHOUSE_PARQUET_DIR = PROJECT_ROOT / "data" / "lakehouse"
# Nouveau dossier (Delta Lake) : utilisé par les jobs Spark.
DELTA_DIR = PROJECT_ROOT / "data" / "delta"

RAW_DIR = PROJECT_ROOT / "data" / "raw"
STATIC_DIR = PROJECT_ROOT / "data" / "static"

BRONZE_DIR = DELTA_DIR / "bronze" / "positions"
SILVER_CLEANED_DIR = DELTA_DIR / "silver" / "positions_cleaned"
SILVER_QUARANTINE_DIR = DELTA_DIR / "silver" / "positions_quarantine"
SILVER_LOCATED_DIR = DELTA_DIR / "silver" / "positions_located"
SILVER_GEO_QUARANTINE_DIR = DELTA_DIR / "silver" / "positions_quarantine_geo"
SILVER_TRACKED_DIR = DELTA_DIR / "silver" / "positions_tracked"

TIMEZONE = "Europe/Brussels"

MAX_DISTANCE_M = 5000

CRS_WGS84 = "EPSG:4326"
CRS_METRIC = "EPSG:31370"  # Lambert belge 72, en mètres

# Petit fichier de référence (Parquet simple), qui reste à son emplacement d'origine.
STOP_OFFSETS_PATH = LAKEHOUSE_PARQUET_DIR / "silver" / "ref_stop_offsets.parquet"

GOLD_DB_PATH = PROJECT_ROOT / "data" / "gold" / "stib.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"
GOLD_TABLES = ["fct_collection_health", "fct_line_speeds", "fct_headway_regularity"]