from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "lakehouse" / "bronze" / "positions"
SILVER_CLEANED_DIR = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "positions_cleaned"
STATIC_DIR = PROJECT_ROOT / "data" / "static"
SILVER_QUARANTINE_DIR = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "positions_quarantine"

TIMEZONE = "Europe/Brussels"

MAX_DISTANCE_M = 5000  

CRS_WGS84 = "EPSG:4326"
CRS_METRIC = "EPSG:31370"  # Lambert belge 72, en mètres
STOP_OFFSETS_PATH = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "ref_stop_offsets.parquet"

SILVER_LOCATED_DIR = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "positions_located"
SILVER_GEO_QUARANTINE_DIR = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "positions_quarantine_geo"

SILVER_TRACKED_DIR = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "positions_tracked"