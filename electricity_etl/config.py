"""Central settings: paths, API details and zone metadata."""
import os
from pathlib import Path

BASE_URL = "https://api.electricitymaps.com/v4"
ZONE = "FR"
MAX_DAYS_PER_REQUEST = 10  # API limit for hourly data
DATA_DIR = Path(os.getenv("ETL_DATA_DIR", "data"))

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
# Optional cloud storage for the data products, e.g. ETL_GOLD_URI=s3://my-bucket/electricity/gold
# (same for ETL_SILVER_URI). AWS credentials come from the standard AWS_* environment variables.
GOLD_DIR = os.getenv("ETL_GOLD_URI") or GOLD_DIR
SILVER_DIR = os.getenv("ETL_SILVER_URI") or SILVER_DIR
STATE_FILE = DATA_DIR / "_state.json"

# dataset name -> API path
DATASETS = {
    "electricity_flows": "/electricity-flows/past-range",
    "electricity_mix": "/electricity-mix/past-range",
}

# Energy sources that count as "production" in the mix
PRODUCTION_SOURCES = ["nuclear", "geothermal", "biomass", "coal", "wind",
                      "solar", "hydro", "gas", "oil", "unknown"]

# Simple grouping used in the Gold table to show the energy transition
SOURCE_CATEGORY = {
    "coal": "fossil", "gas": "fossil", "oil": "fossil",
    "nuclear": "low-carbon",
    "wind": "renewable", "solar": "renewable", "hydro": "renewable",
    "geothermal": "renewable", "biomass": "renewable",
    "unknown": "unknown",
}

# Small hand-made zone lookup (zone metadata for the Gold tables)
ZONE_METADATA = {
    "FR": {"zone_name": "France", "country": "France", "timezone": "Europe/Paris"},
    "BE": {"zone_name": "Belgium", "country": "Belgium", "timezone": "Europe/Brussels"},
    "DE": {"zone_name": "Germany", "country": "Germany", "timezone": "Europe/Berlin"},
    "ES": {"zone_name": "Spain", "country": "Spain", "timezone": "Europe/Madrid"},
    "IT-NO": {"zone_name": "Italy North", "country": "Italy", "timezone": "Europe/Rome"},
    "CH": {"zone_name": "Switzerland", "country": "Switzerland", "timezone": "Europe/Zurich"},
    "GB": {"zone_name": "Great Britain", "country": "United Kingdom", "timezone": "Europe/London"},
}
