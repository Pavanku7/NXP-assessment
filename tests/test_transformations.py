"""Unit tests for the transformation logic (no network needed)."""
from datetime import datetime, timezone

import pandas as pd

from electricity_etl import gold, silver
from electricity_etl.api_client import split_range

REC_FLOWS = {
    "ingestion_timestamp": "2025-03-05T10:00:00+00:00", "source_url": "http://x",
    "payload": {"zone": "FR", "data": [
        {"datetime": "2025-03-01T00:00:00.000Z", "updatedAt": "2025-03-01T01:00:00.000Z",
         "import": {"DE": 100}, "export": {"GB": 40}},
        # same hour delivered again with a corrected value -> must win
        {"datetime": "2025-03-01T00:00:00.000Z", "updatedAt": "2025-03-02T01:00:00.000Z",
         "import": {"DE": 120}, "export": {"GB": 40}},
    ]}}


def flows_df():
    rows = silver.flatten_flows([REC_FLOWS])
    return silver.build(rows, silver.FLOWS_SCHEMA, silver.FLOWS_KEYS)


def test_flatten_flows_gives_one_row_per_neighbour_and_direction():
    assert len(silver.flatten_flows([REC_FLOWS])) == 4


def test_dedup_keeps_latest_update():
    df = flows_df()
    assert len(df) == 2
    assert df[df["neighbour_zone"] == "DE"]["power_mw"].item() == 120


def test_types_and_partitions():
    df = flows_df()
    assert str(df["datetime"].dtype) == "datetime64[us, UTC]"
    assert df["power_mw"].dtype == "float64"
    assert (df["year"][0], df["month"][0], df["day"][0]) == ("2025", "03", "01")


def test_missing_key_rows_are_dropped():
    rec = {**REC_FLOWS, "payload": {"zone": "FR", "data": [
        {"datetime": None, "updatedAt": None, "import": {"DE": 1}}]}}
    rows = silver.flatten_flows([rec])
    df = silver.apply_schema(rows, silver.FLOWS_SCHEMA)
    assert len(silver.check_quality(df, silver.FLOWS_KEYS)) == 0


def mix_df():
    items = [{"datetime": f"2025-03-01T0{h}:00:00.000Z", "updatedAt": "2025-03-02T00:00:00.000Z",
              "breakdownType": "normal", "isEstimated": False, "estimationMethod": None,
              "mix": {"nuclear": 300, "wind": 100, "gas": 100, "hydro storage": {"charge": 5, "discharge": 6},
                      "flows": {"imports": 1, "exports": 2}}} for h in range(2)]
    rec = {"ingestion_timestamp": "2025-03-05T10:00:00+00:00", "source_url": "u",
           "payload": {"zone": "FR", "data": items}}
    return silver.build(silver.flatten_mix([rec]), silver.MIX_SCHEMA, silver.MIX_KEYS)


def test_daily_mix_percentages_add_to_100():
    out = gold.daily_mix(mix_df())
    assert abs(out["share_pct"].sum() - 100) < 0.05
    nuclear = out[out["energy_source"] == "nuclear"]
    assert nuclear["energy_mwh"].item() == 600      # 300 MW x 2 hours
    assert nuclear["share_pct"].item() == 60.0
    assert nuclear["zone_name"].item() == "France"


def test_net_flows_split_into_imports_and_exports():
    rows = []
    for h in range(2):
        rows += [
            {"zone": "FR", "datetime": f"2025-03-01T0{h}:00:00.000Z", "direction": "import",
             "neighbour_zone": "DE", "power_mw": 100.0, "updated_at": "2025-03-02T00:00:00.000Z",
             "ingested_at": "2025-03-05T10:00:00+00:00", "source_url": "u"},
            {"zone": "FR", "datetime": f"2025-03-01T0{h}:00:00.000Z", "direction": "export",
             "neighbour_zone": "DE", "power_mw": 30.0, "updated_at": "2025-03-02T00:00:00.000Z",
             "ingested_at": "2025-03-05T10:00:00+00:00", "source_url": "u"},
            {"zone": "FR", "datetime": f"2025-03-01T0{h}:00:00.000Z", "direction": "export",
             "neighbour_zone": "GB", "power_mw": 50.0, "updated_at": "2025-03-02T00:00:00.000Z",
             "ingested_at": "2025-03-05T10:00:00+00:00", "source_url": "u"}]
    df = silver.build(rows, silver.FLOWS_SCHEMA, silver.FLOWS_KEYS)
    imports, exports = gold.daily_net_flows(df)
    assert imports["net_import_mwh"].tolist() == [140.0]     # (100-30) x 2h
    assert exports["net_export_mwh"].tolist() == [100.0]     # 50 x 2h
    assert exports["destination_zone"].tolist() == ["GB"]


def test_split_range_respects_api_limit():
    s = datetime(2025, 1, 1, tzinfo=timezone.utc)
    e = datetime(2025, 1, 26, tzinfo=timezone.utc)
    chunks = list(split_range(s, e))
    assert len(chunks) == 3 and (chunks[0][1] - chunks[0][0]).days == 10
    assert chunks[-1][1] == e
