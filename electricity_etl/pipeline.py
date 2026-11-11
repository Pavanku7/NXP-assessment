"""Run the whole pipeline: python -m electricity_etl.pipeline --help"""
import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from . import bronze, config, gold, silver
from .api_client import ElectricityMapsClient
from .mock_api import MockClient

log = logging.getLogger("pipeline")


def parse_dt(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def load_state() -> dict:
    if config.STATE_FILE.exists():
        return json.loads(config.STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    config.STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    config.STATE_FILE.write_text(json.dumps(state, indent=2))


def run_bronze(client, start, end, zone):
    """Incremental: if no --start is given, continue from where the last run stopped."""
    state = load_state()
    for dataset in config.DATASETS:
        s = start or (parse_dt(state[dataset]) if dataset in state else end - timedelta(days=7))
        if s >= end:
            log.info("%s: already up to date", dataset)
            continue
        bronze.ingest(client, dataset, zone, s, end)
        state[dataset] = end.strftime("%Y-%m-%dT%H:%M:%S")
        save_state(state)  # saved per dataset so a crash does not lose progress


def main(argv=None):
    p = argparse.ArgumentParser(description="Electricity Maps ETL (Bronze -> Silver -> Gold)")
    p.add_argument("--stages", default="bronze,silver,gold", help="comma list, e.g. silver,gold")
    p.add_argument("--start", help="UTC start, e.g. 2025-01-01T00:00:00 (default: continue from last run, "
                                   "or last 7 days on first run)")
    p.add_argument("--end", help="UTC end (default: current hour)")
    p.add_argument("--zone", default=config.ZONE)
    p.add_argument("--mock", action="store_true", help="use fake data instead of the real API")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    stages = args.stages.split(",")
    if "bronze" in stages:
        if args.mock:
            client = MockClient()
        else:
            try:
                from dotenv import load_dotenv
                load_dotenv()
            except ImportError:
                pass
            key = os.getenv("ELECTRICITYMAPS_API_KEY")
            if not key:
                p.error("Set ELECTRICITYMAPS_API_KEY (see .env.example) or use --mock")
            client = ElectricityMapsClient(key)
        end = parse_dt(args.end) if args.end else datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0)
        run_bronze(client, parse_dt(args.start) if args.start else None, end, args.zone)
    if "silver" in stages:
        silver.run_flows()
        silver.run_mix()
    if "gold" in stages:
        gold.run()
    log.info("Done.")


if __name__ == "__main__":
    main()
