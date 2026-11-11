"""Small helper to write Delta Lake tables (Parquet files + transaction log)."""
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

log = logging.getLogger(__name__)


def is_remote(path) -> bool:
    return "://" in str(path)


def join(root, name: str):
    """root / name that also works for cloud URIs such as s3://bucket/gold."""
    return f"{str(root).rstrip('/')}/{name}" if is_remote(root) else Path(root) / name


def storage_options() -> dict | None:
    """Extra settings for cloud storage. AWS credentials come from the normal AWS_* env variables."""
    return {"AWS_S3_ALLOW_UNSAFE_RENAME": "true"} if os.getenv("ETL_S3_UNSAFE_RENAME", "1") == "1" else None


def read(path) -> pd.DataFrame:
    return DeltaTable(str(path), storage_options=storage_options() if is_remote(path) else None).to_pandas()


def upsert(df: pd.DataFrame, path, keys: list[str], partition_by: list[str], contract=None) -> None:
    """Insert new rows, update rows that already exist (same keys).

    Running the pipeline twice therefore never creates duplicates.
    """
    remote = is_remote(path)
    opts = storage_options() if remote else None
    if not remote:
        path = Path(path)
    if df.empty:
        log.warning("Nothing to write to %s", path)
        return
    if contract is not None:
        from .contracts import validate
        validate(df, contract)  # stop before publishing bad data
    data = pa.Table.from_pandas(df.reset_index(drop=True), preserve_index=False)
    if not DeltaTable.is_deltatable(str(path), storage_options=opts):
        if not remote:
            path.mkdir(parents=True, exist_ok=True)
        write_deltalake(str(path), data, partition_by=partition_by, storage_options=opts)
        log.info("Created Delta table %s (%d rows)", path, len(df))
        return
    predicate = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable(str(path), storage_options=opts)
     .merge(data, predicate=predicate, source_alias="s", target_alias="t")
     .when_matched_update_all()
     .when_not_matched_insert_all()
     .execute())
    log.info("Merged %d rows into %s", len(df), path)
