"""Gold: business-ready daily tables built from Silver.

Data product 1: daily relative electricity mix
Data product 2: daily net imports and net exports (MWh)

Hourly data in MW -> each hourly value counts as MWh for that 1-hour slot.
Days are UTC days.
"""
import logging
from datetime import datetime, timezone

import pandas as pd

from . import config, delta_io

log = logging.getLogger(__name__)
PARTITIONS = ["year", "month"]


def _add_zone_meta(df: pd.DataFrame, zone_col: str, prefix: str) -> pd.DataFrame:
    """Add <prefix>zone_name / country / timezone columns from our small lookup."""
    df = df.copy()
    for field in ("zone_name", "country", "timezone"):
        mapping = {z: m[field] for z, m in config.ZONE_METADATA.items()}
        name = f"{prefix}{field}" if field != "zone_name" else f"{prefix}zone_name"
        df[name] = df[zone_col].map(mapping).astype("string")
    return df


def _finish(df: pd.DataFrame) -> pd.DataFrame:
    """Add year/month partitions and the time this Gold row was created."""
    df = df.copy()
    date = pd.to_datetime(df["date"])
    df["year"] = date.dt.strftime("%Y")
    df["month"] = date.dt.strftime("%m")
    df["gold_created_at"] = pd.Timestamp(datetime.now(timezone.utc)).as_unit("us")
    return df


# ---------- Product 1 ----------
def daily_mix(silver_mix: pd.DataFrame) -> pd.DataFrame:
    sources = config.PRODUCTION_SOURCES
    df = silver_mix[silver_mix["breakdown_type"] == "normal"].copy()
    df["date"] = df["datetime"].dt.date

    # one row per hour and source
    long = df.melt(id_vars=["zone", "date", "datetime"], value_vars=[f"{s}_mw" for s in sources],
                   var_name="energy_source", value_name="mw").dropna(subset=["mw"])
    long["energy_source"] = long["energy_source"].str.replace(r"_mw$", "", regex=True)

    # add up the hours of each day
    daily = (long.groupby(["zone", "date", "energy_source"], as_index=False)
             .agg(energy_mwh=("mw", "sum"), hours_covered=("datetime", "nunique")))

    # share of each source in that day's total
    day_total = daily.groupby(["zone", "date"])["energy_mwh"].transform("sum")
    daily["share_pct"] = (daily["energy_mwh"] / day_total * 100).round(2)
    daily["source_category"] = daily["energy_source"].map(config.SOURCE_CATEGORY).fillna("unknown")
    daily["reference_date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y-%m-%d")

    out = _finish(_add_zone_meta(daily, "zone", ""))
    cols = ["zone", "zone_name", "country", "timezone", "date", "reference_date",
            "energy_source", "source_category", "energy_mwh", "share_pct", "hours_covered",
            "gold_created_at", "year", "month"]
    return out[cols].sort_values(["date", "energy_source"]).reset_index(drop=True)


# ---------- Product 2 ----------
def daily_net_flows(silver_flows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = silver_flows.copy()
    df["date"] = df["datetime"].dt.date
    keys = ["zone", "date", "neighbour_zone"]

    # MWh imported and exported per neighbour and day
    power = (df.groupby(keys + ["direction"])["power_mw"].sum().unstack("direction")
             .reindex(columns=["import", "export"]).fillna(0.0))
    daily = power.rename(columns={"import": "imported_mwh", "export": "exported_mwh"})
    daily["hours_covered"] = df.groupby(keys)["datetime"].nunique()
    daily["net_mwh"] = daily["imported_mwh"] - daily["exported_mwh"]
    daily = daily.reset_index()

    # imports: neighbour -> France
    imp = daily[daily["net_mwh"] > 0].rename(
        columns={"neighbour_zone": "source_zone", "zone": "destination_zone", "net_mwh": "net_import_mwh"})
    imp = _add_zone_meta(imp, "destination_zone", "destination_")
    imp = _add_zone_meta(imp, "source_zone", "source_")

    # exports: France -> neighbour (shown as a positive number)
    exp = daily[daily["net_mwh"] < 0].copy()
    exp["net_export_mwh"] = -exp["net_mwh"]
    exp = exp.drop(columns="net_mwh").rename(
        columns={"neighbour_zone": "destination_zone", "zone": "source_zone"})
    exp = _add_zone_meta(exp, "source_zone", "source_")
    exp = _add_zone_meta(exp, "destination_zone", "destination_")

    imp = _finish(imp)[[
        "source_zone", "source_zone_name", "source_country", "destination_zone",
        "destination_zone_name", "date", "net_import_mwh", "imported_mwh", "exported_mwh",
        "hours_covered", "gold_created_at", "year", "month"]]
    exp = _finish(exp)[[
        "source_zone", "source_zone_name", "destination_zone", "destination_zone_name",
        "destination_country", "date", "net_export_mwh", "imported_mwh", "exported_mwh",
        "hours_covered", "gold_created_at", "year", "month"]]
    return (imp.sort_values(["date", "source_zone"]).reset_index(drop=True),
            exp.sort_values(["date", "destination_zone"]).reset_index(drop=True))


# ---------- entry point ----------
def run(silver_root=config.SILVER_DIR, gold_root=config.GOLD_DIR) -> None:
    from . import contracts as c
    mix = delta_io.read(delta_io.join(silver_root, "electricity_mix"))
    flows = delta_io.read(delta_io.join(silver_root, "electricity_flows"))

    delta_io.upsert(daily_mix(mix), delta_io.join(gold_root, "daily_electricity_mix"),
                    ["zone", "date", "energy_source"], PARTITIONS, c.GOLD_MIX)
    imports, exports = daily_net_flows(flows)
    delta_io.upsert(imports, delta_io.join(gold_root, "daily_electricity_imports"),
                    ["source_zone", "destination_zone", "date"], PARTITIONS, c.GOLD_IMPORTS)
    delta_io.upsert(exports, delta_io.join(gold_root, "daily_electricity_exports"),
                    ["source_zone", "destination_zone", "date"], PARTITIONS, c.GOLD_EXPORTS)
