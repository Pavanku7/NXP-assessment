# Electricity Maps ETL – France (FR)

A Python pipeline that downloads hourly electricity data for France from the
[Electricity Maps API](https://app.electricitymaps.com/docs) and organises it in three layers
(Bronze → Silver → Gold) so analysts can study France's energy mix and its cross-border trade.

Stack: **Pandas + Delta-RS (`deltalake`)**.

## 1. Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows   (Linux/Mac: source .venv/bin/activate)
pip install -r requirements.txt
copy .env.example .env            # then paste your API key into .env
```

API key: the old "sandbox" key has been retired by Electricity Maps; a free 14-day trial key
(from your account at electricitymaps.com) works with the real API.

## 2. Run

```bash
# everything, for a chosen period
python -m electricity_etl.pipeline --start 2025-03-01T00:00:00 --end 2025-03-08T00:00:00

# daily/incremental use: no dates -> continues from where the last run stopped
python -m electricity_etl.pipeline

# only rebuild Silver and Gold from existing Bronze files
python -m electricity_etl.pipeline --stages silver,gold

# no API key? run on synthetic (fake, random) data to see the full flow
python -m electricity_etl.pipeline --mock --start 2025-03-01T00:00:00 --end 2025-03-04T00:00:00

# tests
python -m pytest
```

Output goes to `data/` (change with the `ETL_DATA_DIR` environment variable).

## 3. How it is built

| Layer | What it holds | Format | Partitioned by |
|---|---|---|---|
| Bronze | API answers exactly as received + `ingestion_timestamp` + `source_url` | JSON files | `year/month/day` of **ingestion** |
| Silver | Cleaned, flat, typed, de-duplicated tables | Delta Lake (Parquet files) | `year/month/day` of the **data** timestamp |
| Gold | Daily, analysis-ready tables | Delta Lake (Parquet files) | `year/month` of the reference date |

```
electricity_etl/
  config.py      settings, paths, zone lookup
  api_client.py  API calls, retry with waiting on rate limits (429) and server errors, 10-day chunking
  mock_api.py    fake API for demos/CI
  bronze.py      store raw responses
  silver.py      flatten, type, quality check, de-duplicate
  gold.py        business tables
  delta_io.py    Delta "upsert" helper
  contracts.py   data contracts (schema + business rules) checked before writing
  pipeline.py    command line entry point
dags/            Airflow DAG
tests/           unit tests for the transformations
.github/workflows/ci.yml   runs tests + a fake-data end-to-end run on every push
```

### Design decisions
- **Bronze is never modified.** One file per API call; the whole answer is kept so we can always rebuild later.
- **API limit:** hourly requests may cover at most 10 days, so longer periods are split into chunks.
- **Retries:** rate-limit and temporary server errors are retried with increasing waits (honours `Retry-After`). Wrong key / bad request fails immediately with a clear error.
- **Incremental:** `_state.json` remembers how far each dataset was loaded; the next run starts from there.
- **Safe re-runs:** Silver and Gold are written with Delta `MERGE` (upsert) on their unique keys, so running twice never duplicates rows.
- **De-duplication:** if the same hour arrives twice (the API corrects past values), the one with the newest `updated_at` wins.
- **Quality check:** rows missing a key field (zone / time / direction / neighbour) are dropped and logged.
- **Units:** the API gives MW (power) per hour. One hour at X MW = X MWh, so daily MWh = sum of the 24 hourly MW values. Days are UTC days. `hours_covered` shows if a day is incomplete.
- **Mix %:** share of each source among *production* sources only (nuclear, wind, solar, hydro, gas, coal, oil, biomass, geothermal, unknown). Storage and import/export are excluded so the shares add to 100 %.
- **Net import/export:** per neighbour and day, `net = imported MWh − exported MWh`. Positive → row in the imports table; negative → row in the exports table (shown as a positive number). Neighbours with exactly zero net are omitted.

## 4. Schemas

### Silver `electricity_flows` (one row per hour × direction × neighbour)
`zone` string · `datetime` timestamp UTC · `direction` (`import`/`export`) · `neighbour_zone` string · `power_mw` double · `updated_at` timestamp · `ingested_at` timestamp · `source_url` string · `year`/`month`/`day` string (partitions).
Unique key: `zone, datetime, direction, neighbour_zone`.

### Silver `electricity_mix` (one row per hour)
`zone`, `datetime`, `breakdown_type`, one `<source>_mw` double for each of nuclear, geothermal, biomass, coal, wind, solar, hydro, gas, oil, unknown, then `hydro_storage_charge_mw`, `hydro_storage_discharge_mw`, `battery_storage_charge_mw`, `battery_storage_discharge_mw`, `flows_imports_mw`, `flows_exports_mw`, `is_estimated` boolean, `estimation_method`, `updated_at`, `ingested_at`, `source_url`, `year`/`month`/`day`.
Unique key: `zone, datetime, breakdown_type`.

### Gold `daily_electricity_mix` (Data Product 1)
`zone, zone_name, country, timezone, date, reference_date, energy_source, source_category (fossil / low-carbon / renewable / unknown), energy_mwh, share_pct, hours_covered, gold_created_at, year, month`.
Key: `zone, date, energy_source`.

### Gold `daily_electricity_imports` and `daily_electricity_exports` (Data Product 2)
Imports: `source_zone, source_zone_name, source_country, destination_zone, destination_zone_name, date, net_import_mwh, imported_mwh, exported_mwh, hours_covered, gold_created_at, year, month`.
Exports: same idea with `destination_country` and `net_export_mwh`.
Key: `source_zone, destination_zone, date`.

## 5. Sample outputs
The `data/` folder holds real output for France, loaded with a **free trial key**. Trial keys cannot use the
`past-range` endpoint (any dates), so the pipeline automatically falls back to `history` (the last 24 hours).
With a paid key it uses `past-range` and loads any period, in 10-day chunks. Because of the 24-hour limit,
the sample covers only about one day, so the first and last UTC day are partial (see `hours_covered`).

## 6. Bonus items covered
| Bonus | Where |
|---|---|
| Cloud storage (S3) | Set `ETL_GOLD_URI=s3://bucket/path/gold` (and optionally `ETL_SILVER_URI`) plus the usual `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`. Delta tables are then written straight to S3 (`delta_io.py`). Tested against a local fake S3 (moto); not against real AWS. |
| Orchestration | `dags/electricity_maps_fr_etl` Airflow DAG (daily, one task per layer, retries) and `.github/workflows/scheduled-run.yml` (daily cron without Airflow). The Airflow DAG was not run on a live Airflow instance. |
| Unit tests | `tests/` (`python -m pytest`) |
| Data quality + data contracts | Silver drops rows with missing keys. `contracts.py` defines columns, types, unique keys and business rules (e.g. mix shares add to 100 %, net MWh = imported - exported, hours 1-24) for every Silver and Gold table; they are checked before each write and the run stops on a violation. |
| Error handling / rate-limit retries | `api_client.py` |
| Incremental ingestion | `_state.json` |
| CI/CD | `.github/workflows/ci.yml`: tests + fake-data end-to-end run on every push/PR; on `main` it builds and publishes a Docker image (`Dockerfile`) to GitHub Container Registry. |

Docker: `docker run --rm -e ELECTRICITYMAPS_API_KEY=... -v $PWD/data:/data ghcr.io/<owner>/<repo>:latest`
