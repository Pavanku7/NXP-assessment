"""Tests for the data contracts and the cloud-path helper."""
from datetime import date

import pandas as pd
import pytest

from electricity_etl import contracts, delta_io, gold, silver
from tests.test_transformations import flows_df


def good_gold_mix():
    mix = pd.DataFrame({
        "zone": ["FR"] * 2,
        "datetime": pd.to_datetime(["2025-03-01T00:00Z", "2025-03-01T01:00Z"]).astype("datetime64[us, UTC]"),
        "breakdown_type": ["normal"] * 2,
        **{f"{s}_mw": [50.0, 50.0] if s in ("nuclear", "wind") else [0.0, 0.0]
           for s in silver.config.PRODUCTION_SOURCES}})
    return gold.daily_mix(mix)


def test_gold_mix_passes_contract():
    contracts.validate(good_gold_mix(), contracts.GOLD_MIX)


def test_gold_mix_bad_share_is_rejected():
    df = good_gold_mix()
    df.loc[0, "share_pct"] = 150.0
    with pytest.raises(contracts.DataContractError, match="share_pct"):
        contracts.validate(df, contracts.GOLD_MIX)


def test_duplicate_keys_are_rejected():
    df = good_gold_mix()
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    with pytest.raises(contracts.DataContractError, match="duplicate"):
        contracts.validate(df, contracts.GOLD_MIX)


def test_missing_column_is_rejected():
    with pytest.raises(contracts.DataContractError, match="missing columns"):
        contracts.validate(good_gold_mix().drop(columns="zone_name"), contracts.GOLD_MIX)


def test_silver_flows_contract_and_negative_power():
    df = flows_df()
    contracts.validate(df, contracts.SILVER_FLOWS)
    df.loc[0, "power_mw"] = -5.0
    with pytest.raises(contracts.DataContractError, match="power_mw"):
        contracts.validate(df, contracts.SILVER_FLOWS)


def test_gold_flows_pass_contract():
    imp, exp = gold.daily_net_flows(flows_df())
    contracts.validate(imp, contracts.GOLD_IMPORTS)
    contracts.validate(exp, contracts.GOLD_EXPORTS)


def test_join_handles_s3_uri_and_local_path():
    assert delta_io.join("s3://bucket/gold/", "t") == "s3://bucket/gold/t"
    assert str(delta_io.join("data/gold", "t")).replace("\\", "/") == "data/gold/t"
