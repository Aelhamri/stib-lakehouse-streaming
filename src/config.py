from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "lakehouse" / "bronze" / "positions"
SILVER_CLEANED_DIR = PROJECT_ROOT / "data" / "lakehouse" / "silver" / "positions_cleaned"
STATIC_DIR = PROJECT_ROOT / "data" / "static"

TIMEZONE = "Europe/Brussels"