"""Data contracts: the agreed columns, types and rules for every Silver/Gold table.

A contract is checked right before a table is written. If a rule is broken the
pipeline stops with a clear message instead of publishing bad data.
"""
import logging
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd
from pandas.api import types as pt

from . import config
from .silver import FLOWS_KEYS, FLOWS_SCHEMA, MIX_KEYS, MIX_SCHEMA

log = logging.getLogger(__name__)


class DataContractError(ValueError):
    """Raised when a table breaks its contract."""


KIND_CHECKS: dict[str, Callable] = {
    "str": lambda s: pt.is_string_dtype(s) or pt.is_object_dtype(s),
    "float": pt.is_float_dtype,
    "int": pt.is_integer_dtype,
    "bool": lambda s: pt.is_bool_dtype(s),
    "ts": pt.is_datetime64_any_dtype,
    "date": lambda s: pt.is_object_dtype(s) or pt.is_datetime64_any_dtype(s),
}


@dataclass
class Contract:
    name: str
    columns: dict[str, str]                 # column -> kind
    keys: list[str]                         # must be unique and never null
    rules: list[tuple[str, Callable[[pd.DataFrame], pd.Series]]] = field(default_factory=list)
    # each rule: (description, function returning a boolean Series that is True for GOOD rows)


def validate(df: pd.DataFrame, contract: Contract) -> None:
    """Raise DataContractError listing every broken rule (does nothing if all is fine)."""
    problems = []
    missing = [c for c in contract.columns if c not in df.columns]
    if missing:
        raise DataContractError(f"[{contract.name}] missing columns: {missing}")
    extra = [c for c in df.columns if c not in contract.columns]
    if extra:
        problems.append(f"unexpected columns: {extra}")
    for col, kind in contract.columns.items():
        if not KIND_CHECKS[kind](df[col]):
            problems.append(f"column '{col}' should be {kind} but is {df[col].dtype}")
    for k in contract.keys:
        if df[k].isna().any():
            problems.append(f"key column '{k}' has {int(df[k].isna().sum())} nulls")
    dups = int(df.duplicated(subset=contract.keys).sum())
    if dups:
        problems.append(f"{dups} duplicate rows for key {contract.keys}")
    for text, rule in contract.rules:
        bad = int((~rule(df).fillna(False)).sum())
        if bad:
            problems.append(f"{bad} rows violate rule: {text}")
    if problems:
        raise DataContractError(f"[{contract.name}] " + "; ".join(problems))
    log.info("Data contract OK: %s (%d rows)", contract.name, len(df))


# ---------- Silver ----------
_PART = {"year": "str", "month": "str", "day": "str"}

SILVER_FLOWS = Contract(
    "silver.electricity_flows", {**FLOWS_SCHEMA, **_PART}, FLOWS_KEYS,
    [("direction is import or export", lambda d: d["direction"].isin(["import", "export"])),
     ("power_mw is not negative", lambda d: d["power_mw"].isna() | (d["power_mw"] >= 0))])

SILVER_MIX = Contract(
    "silver.electricity_mix", {**MIX_SCHEMA, **_PART}, MIX_KEYS,
    [("production values are not negative",
      lambda d: (d[[f"{s}_mw" for s in config.PRODUCTION_SOURCES]].fillna(0) >= 0).all(axis=1))])

# ---------- Gold ----------
_HOURS = ("hours_covered is between 1 and 24", lambda d: d["hours_covered"].between(1, 24))
_META = {"gold_created_at": "ts", "year": "str", "month": "str"}

GOLD_MIX = Contract(
    "gold.daily_electricity_mix",
    {"zone": "str", "zone_name": "str", "country": "str", "timezone": "str", "date": "date",
     "reference_date": "str", "energy_source": "str", "source_category": "str",
     "energy_mwh": "float", "share_pct": "float", "hours_covered": "int", **_META},
    ["zone", "date", "energy_source"],
    [("energy_mwh is not negative", lambda d: d["energy_mwh"] >= 0),
     ("share_pct is between 0 and 100", lambda d: d["share_pct"].between(0, 100)),
     ("shares of each day add up to 100 (+/- 0.5)",
      lambda d: d.groupby(["zone", "date"])["share_pct"].transform("sum").sub(100).abs() <= 0.5),
     ("energy_source is a known production source", lambda d: d["energy_source"].isin(config.PRODUCTION_SOURCES)),
     ("zone metadata is filled", lambda d: d["zone_name"].notna()),
     _HOURS])

_FLOW_COMMON = {"source_zone": "str", "source_zone_name": "str", "destination_zone": "str",
                "destination_zone_name": "str", "date": "date", "imported_mwh": "float",
                "exported_mwh": "float", "hours_covered": "int", **_META}

GOLD_IMPORTS = Contract(
    "gold.daily_electricity_imports",
    {**_FLOW_COMMON, "source_country": "str", "net_import_mwh": "float"},
    ["source_zone", "destination_zone", "date"],
    [("net_import_mwh is positive", lambda d: d["net_import_mwh"] > 0),
     ("net = imported - exported",
      lambda d: (d["imported_mwh"] - d["exported_mwh"] - d["net_import_mwh"]).abs() < 1e-6),
     ("zone metadata is filled", lambda d: d["source_zone_name"].notna() & d["destination_zone_name"].notna()),
     _HOURS])

GOLD_EXPORTS = Contract(
    "gold.daily_electricity_exports",
    {**_FLOW_COMMON, "destination_country": "str", "net_export_mwh": "float"},
    ["source_zone", "destination_zone", "date"],
    [("net_export_mwh is positive", lambda d: d["net_export_mwh"] > 0),
     ("net = exported - imported",
      lambda d: (d["exported_mwh"] - d["imported_mwh"] - d["net_export_mwh"]).abs() < 1e-6),
     ("zone metadata is filled", lambda d: d["source_zone_name"].notna() & d["destination_zone_name"].notna()),
     _HOURS])
