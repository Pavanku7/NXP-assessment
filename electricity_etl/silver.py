"""Silver: read Bronze JSON, flatten, type, check quality, remove duplicates, save as Delta."""
import json
import logging
from pathlib import Path

import pandas as pd

from . import config, delta_io

log = logging.getLogger(__name__)

# column -> kind: "str", "ts" (timestamp), "float", "bool"
FLOWS_SCHEMA = {
    "zone": "str", "datetime": "ts", "direction": "str", "neighbour_zone": "str",
    "power_mw": "float", "updated_at": "ts", "ingested_at": "ts", "source_url": "str",
}
FLOWS_KEYS = ["zone", "datetime", "direction", "neighbour_zone"]

STORAGE = {"hydro storage": "hydro_storage", "battery storage": "battery_storage"}
MIX_SCHEMA = {
    "zone": "str", "datetime": "ts", "breakdown_type": "str",
    **{f"{s}_mw": "float" for s in config.PRODUCTION_SOURCES},
    **{f"{n}_{d}_mw": "float" for n in STORAGE.values() for d in ("charge", "discharge")},
    "flows_imports_mw": "float", "flows_exports_mw": "float",
    "is_estimated": "bool", "estimation_method": "str",
    "updated_at": "ts", "ingested_at": "ts", "source_url": "str",
}
MIX_KEYS = ["zone", "datetime", "breakdown_type"]
PARTITIONS = ["year", "month", "day"]


# ---------- reading Bronze ----------
def read_bronze(dataset: str, root: Path = config.BRONZE_DIR) -> list[dict]:
    files = sorted((root / dataset).rglob("*.json"))
    log.info("Silver: reading %d Bronze files for %s", len(files), dataset)
    return [json.loads(f.read_text(encoding="utf-8")) for f in files]


# ---------- flattening (plain functions, easy to unit test) ----------
def flatten_flows(records: list[dict]) -> list[dict]:
    """Nested import/export dictionaries -> one row per hour, direction and neighbour."""
    rows = []
    for rec in records:
        payload = rec["payload"]
        for item in payload.get("data") or payload.get("history") or []:
            for direction in ("import", "export"):
                for neighbour, mw in (item.get(direction) or {}).items():
                    rows.append({
                        "zone": payload["zone"], "datetime": item["datetime"],
                        "direction": direction, "neighbour_zone": neighbour,
                        "power_mw": mw, "updated_at": item.get("updatedAt"),
                        "ingested_at": rec["ingestion_timestamp"], "source_url": rec["source_url"],
                    })
    return rows


def flatten_mix(records: list[dict]) -> list[dict]:
    """Nested 'mix' dictionary -> one flat row per hour."""
    rows = []
    for rec in records:
        payload = rec["payload"]
        for item in payload.get("data") or payload.get("history") or []:
            mix = item.get("mix") or {}
            row = {"zone": payload["zone"], "datetime": item["datetime"],
                   "breakdown_type": item.get("breakdownType", "normal"),
                   "is_estimated": item.get("isEstimated"),
                   "estimation_method": item.get("estimationMethod"),
                   "updated_at": item.get("updatedAt"),
                   "ingested_at": rec["ingestion_timestamp"], "source_url": rec["source_url"]}
            for s in config.PRODUCTION_SOURCES:
                row[f"{s}_mw"] = mix.get(s)
            for api_name, col in STORAGE.items():
                for d in ("charge", "discharge"):
                    row[f"{col}_{d}_mw"] = (mix.get(api_name) or {}).get(d)
            row["flows_imports_mw"] = (mix.get("flows") or {}).get("imports")
            row["flows_exports_mw"] = (mix.get("flows") or {}).get("exports")
            rows.append(row)
    return rows


# ---------- typing, cleaning, partition columns ----------
def apply_schema(rows: list[dict], schema: dict) -> pd.DataFrame:
    """Enforce data types (timestamps arrive as text, so we convert them)."""
    df = pd.DataFrame(rows, columns=list(schema))
    for col, kind in schema.items():
        if kind == "ts":
            df[col] = pd.to_datetime(df[col], utc=True).astype("datetime64[us, UTC]")
        elif kind == "float":
            df[col] = pd.to_numeric(df[col]).astype("float64")
        elif kind == "bool":
            df[col] = df[col].astype("boolean")
        else:
            df[col] = df[col].astype("string")
    return df


def check_quality(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Drop rows missing a key field (they cannot be identified) and warn about it."""
    clean = df.dropna(subset=keys)
    if len(clean) < len(df):
        log.warning("Data quality: dropping %d rows with missing key fields", len(df) - len(clean))
    return clean


def deduplicate(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Same key twice (API re-issued a corrected value) -> keep the newest update."""
    newest_first = df.sort_values(["updated_at", "ingested_at"], ascending=False, na_position="last")
    return newest_first.drop_duplicates(subset=keys, keep="first")


def add_partitions(df: pd.DataFrame) -> pd.DataFrame:
    """Partition by the DATA timestamp (not by when we downloaded it)."""
    df = df.copy()
    df["year"] = df["datetime"].dt.strftime("%Y")
    df["month"] = df["datetime"].dt.strftime("%m")
    df["day"] = df["datetime"].dt.strftime("%d")
    return df


def build(rows, schema, keys) -> pd.DataFrame:
    df = apply_schema(rows, schema)
    df = check_quality(df, keys)
    df = deduplicate(df, keys)
    return add_partitions(df).sort_values(keys).reset_index(drop=True)


# ---------- entry points ----------
def run_flows(bronze_root=config.BRONZE_DIR, silver_root=config.SILVER_DIR) -> pd.DataFrame:
    df = build(flatten_flows(read_bronze("electricity_flows", bronze_root)), FLOWS_SCHEMA, FLOWS_KEYS)
    from .contracts import SILVER_FLOWS
    delta_io.upsert(df, delta_io.join(silver_root, "electricity_flows"), FLOWS_KEYS, PARTITIONS, SILVER_FLOWS)
    return df


def run_mix(bronze_root=config.BRONZE_DIR, silver_root=config.SILVER_DIR) -> pd.DataFrame:
    df = build(flatten_mix(read_bronze("electricity_mix", bronze_root)), MIX_SCHEMA, MIX_KEYS)
    from .contracts import SILVER_MIX
    delta_io.upsert(df, delta_io.join(silver_root, "electricity_mix"), MIX_KEYS, PARTITIONS, SILVER_MIX)
    return df
