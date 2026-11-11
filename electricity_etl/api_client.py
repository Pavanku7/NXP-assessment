"""Thin API client with retries (handles rate limits and temporary errors)."""
import logging
import time
from datetime import datetime, timedelta

import requests

from . import config

try:  # use the operating system's certificate store (needed behind corporate proxies)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

log = logging.getLogger(__name__)
RETRY_STATUS = {429, 500, 502, 503, 504}


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def split_range(start: datetime, end: datetime, days: int = config.MAX_DAYS_PER_REQUEST):
    """Cut a long period into chunks the API accepts."""
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


class ElectricityMapsClient:
    def __init__(self, api_key: str, max_retries: int = 5, backoff: float = 2.0):
        self.session = requests.Session()
        self.session.headers["auth-token"] = api_key
        self.max_retries = max_retries
        self.backoff = backoff
        # "past-range" = any dates (paid plans). "history" = last 24 hours only (free trial).
        self.mode = "past-range"

    def fetch(self, dataset: str, zone: str, start: datetime, end: datetime):
        """Return (full_url, parsed_json). Retries on rate limit / server errors."""
        if self.mode == "history":
            url = config.BASE_URL + config.DATASETS[dataset].replace("past-range", "history")
            params = {"zone": zone, "temporalGranularity": "hourly"}
        else:
            url = config.BASE_URL + config.DATASETS[dataset]
            params = {"zone": zone, "start": iso(start), "end": iso(end),
                      "temporalGranularity": "hourly"}
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                log.warning("Network error (%s), attempt %d", exc, attempt)
                time.sleep(self.backoff ** attempt)
                continue
            if resp.status_code == 200:
                return resp.url, resp.json()
            if resp.status_code == 401 and self.mode == "past-range" and "trial" in resp.text:
                log.warning("Your API plan is a trial: 'past-range' is not allowed. "
                            "Falling back to 'history' (last 24 hours only).")
                self.mode = "history"
                return self.fetch(dataset, zone, start, end)
            if resp.status_code not in RETRY_STATUS:
                resp.raise_for_status()  # e.g. 401 bad key, 400 bad request
            wait = float(resp.headers.get("Retry-After", self.backoff ** attempt))
            log.warning("HTTP %s, retrying in %.0fs", resp.status_code, wait)
            time.sleep(wait)
        raise RuntimeError(f"Giving up on {dataset} {start}..{end} after {self.max_retries} tries")
