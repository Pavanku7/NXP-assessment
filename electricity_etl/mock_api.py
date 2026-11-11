"""Fake API for demos/tests when you have no key.

The data is SYNTHETIC (random numbers) but has the same shape as the real API.
"""
import random
from datetime import datetime, timedelta

from . import config
from .api_client import iso


class MockClient:
    def fetch(self, dataset, zone, start: datetime, end: datetime):
        rnd = random.Random(f"{dataset}{start}")
        url = f"mock://{config.DATASETS[dataset]}?zone={zone}&start={iso(start)}&end={iso(end)}"
        data, t = [], start
        while t < end:
            row = {"datetime": t.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                   "updatedAt": (t + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
            if dataset == "electricity_flows":
                row["import"] = {"DE": rnd.randint(0, 2500), "ES": rnd.randint(0, 1500)}
                row["export"] = {"GB": rnd.randint(0, 3500), "IT-NO": rnd.randint(0, 3000),
                                 "CH": rnd.randint(0, 2500), "BE": rnd.randint(0, 2000)}
            else:
                mix = {s: rnd.randint(0, 40000) if s == "nuclear" else rnd.randint(0, 6000)
                       for s in config.PRODUCTION_SOURCES}
                mix["hydro storage"] = {"charge": rnd.randint(0, 3000), "discharge": rnd.randint(0, 3000)}
                mix["battery storage"] = {"charge": 0, "discharge": 0}
                mix["flows"] = {"imports": rnd.randint(0, 4000), "exports": rnd.randint(0, 12000)}
                row.update(breakdownType="normal", mix=mix, isEstimated=False, estimationMethod=None)
            data.append(row)
            t += timedelta(hours=1)
        return url, {"zone": zone, "temporalGranularity": "hourly", "unit": "MW", "data": data}
