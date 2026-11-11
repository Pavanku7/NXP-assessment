"""Bronze: save the API answer exactly as received, plus 2 metadata fields."""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .api_client import split_range

log = logging.getLogger(__name__)


def bronze_path(dataset: str, ingested_at: datetime, start: datetime, end: datetime,
                root: Path = config.BRONZE_DIR) -> Path:
    """Folder year=YYYY/month=MM/day=DD is based on the INGESTION time."""
    folder = (root / dataset / f"year={ingested_at:%Y}" / f"month={ingested_at:%m}"
              / f"day={ingested_at:%d}")
    name = f"{dataset}_{start:%Y%m%dT%H}_{end:%Y%m%dT%H}_ingested{ingested_at:%H%M%S%f}.json"
    return folder / name


def ingest(client, dataset: str, zone: str, start: datetime, end: datetime,
           root: Path = config.BRONZE_DIR) -> list[Path]:
    """Call the API (in 10-day chunks) and store every answer as one JSON file."""
    written = []
    for s, e in split_range(start, end):
        url, payload = client.fetch(dataset, zone, s, e)
        now = datetime.now(timezone.utc)
        record = {"ingestion_timestamp": now.isoformat(), "source_url": url, "payload": payload}
        path = bronze_path(dataset, now, s, e, root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record), encoding="utf-8")  # raw, never changed afterwards
        log.info("Bronze: wrote %s", path)
        written.append(path)
        if getattr(client, "mode", "") == "history":
            break  # 'history' always returns the last 24h, so one call is enough
    return written
