# Change Log – Bonus Items Added

Reference for the changes made to the project after the base Bronze → Silver → Gold pipeline
was reviewed against the assignment PDF (`req-arch/Data Engineer - Technical Assignment.pdf`).

## 1. Review result (before the changes)

- Mandatory items (Bronze/Silver/Gold, Delta + Parquet, partitioning, README, sample data): all present.
- Tests: 7 passed. Mock run (`--mock`) worked end to end and re-runs did not duplicate rows.
- Live run with the trial key worked: trial keys cannot use `past-range`, so the client falls back to
  `history` (last 24 h). Output: Silver 170 flows / 24 mix rows; Gold 16 mix / 2 imports / 12 exports rows.
- Bonus items missing: S3 storage, orchestration, explicit data contracts, deploy step in CI.

## 2. What was added

### Summary

| Bonus item | What I added |
|---|---|
| S3 storage | Set `ETL_GOLD_URI=s3://bucket/path` (and optionally `ETL_SILVER_URI`) plus the usual `AWS_*` credentials, and the Delta tables are written to S3. I ran the pipeline twice against a local fake S3 (moto). Both runs wrote the tables and the second run merged without duplicating rows. |
| Orchestration | `dags/electricity_maps_dag.py` is a daily Airflow DAG with one task per layer and 3 retries. `scheduled-run.yml` is a daily cron that doesn't need Airflow. |
| Data contracts | `contracts.py` defines columns, types, unique keys and business rules for all 5 Silver and Gold tables. For example, mix shares must add up to 100%, net MWh must equal imported minus exported, and hours covered must be between 1 and 24. A table is checked before it is written, and a violation stops the run. |
| CI/CD | `ci.yml` now runs the tests and the mock end-to-end run. On `main` it also builds the new `Dockerfile` and publishes the image to GitHub Container Registry. |
| README | The bonus section is rewritten to describe all of this. |

Details per file follow.

### 2.1 Data contracts – `electricity_etl/contracts.py` (new)
- `Contract` = columns + types, unique keys, business rules. `validate(df, contract)` raises
  `DataContractError` listing every broken rule.
- Contracts: `SILVER_FLOWS`, `SILVER_MIX`, `GOLD_MIX`, `GOLD_IMPORTS`, `GOLD_EXPORTS`.
- Example rules: no nulls/duplicates in keys, power not negative, mix shares per day add up to 100 (±0.5),
  `net = imported − exported`, `hours_covered` between 1 and 24, zone metadata filled.
- Checked **before** each write, so a violation stops the run without publishing bad data.

### 2.2 Wiring contracts + S3 – `delta_io.py`, `silver.py`, `gold.py`, `config.py`
- `delta_io.upsert(..., contract=None)` validates first when a contract is passed.
- `delta_io.join(root, name)` builds paths for both local folders and URIs (`s3://...`);
  `delta_io.read/upsert` pass `storage_options` for remote paths.
- `config.py`: optional env vars `ETL_GOLD_URI` and `ETL_SILVER_URI` (e.g. `s3://bucket/path/gold`) override
  the local folders. AWS credentials come from the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_REGION`. `ETL_S3_UNSAFE_RENAME` (default `1`) sets `AWS_S3_ALLOW_UNSAFE_RENAME` because plain S3 has no
  locking; set it to `0` if you use a DynamoDB locking setup.
- Bronze stays local (it writes JSON files directly).
- `silver.py` and `gold.py` import contracts inside the `run*` functions (avoids a circular import,
  because `contracts.py` imports the Silver schemas).

### 2.3 Orchestration
- `dags/electricity_maps_dag.py`: Airflow DAG `electricity_maps_fr_etl`, daily 02:00 UTC, tasks
  `bronze >> silver >> gold`, 3 retries, `catchup=False`.
- `.github/workflows/scheduled-run.yml`: daily cron (02:00 UTC) running `python -m electricity_etl.pipeline`.
  Needs repository secrets `ELECTRICITYMAPS_API_KEY` and, for S3 output, `ETL_GOLD_URI`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.
  Note: GitHub runners do not keep `data/` between runs, so incremental state is only kept when output goes
  to persistent storage; for a real schedule use S3 or Airflow with a persistent disk.

### 2.4 CI/CD and container
- `.github/workflows/ci.yml` (rewritten): `test` job (pytest + mock end-to-end run, which also runs the
  contract checks) on push/PR; `deploy` job builds and pushes `ghcr.io/<repo>:latest` on `main`.
- `Dockerfile` (python:3.12-slim, `ETL_DATA_DIR=/data`) and `.dockerignore` (new).

### 2.5 Tests – `tests/test_contracts.py` (new), `tests/__init__.py` (new)
7 new tests: valid Gold mix passes, bad share rejected, duplicate keys rejected, missing column rejected,
Silver flows contract + negative power rejected, Gold flows pass, `join()` handles S3 URIs and local paths.
Total: 14 tests, all passing.

### 2.6 README
Section 6 rewritten as a bonus table; module list now mentions `contracts.py` and `dags/`; Docker usage added.

## 3. How it was verified

| Check | Result |
|---|---|
| `python -m pytest` | 14 passed |
| Mock run (`--mock`, 2025-03-01 → 03-04) | 5 contract checks OK (2 Silver, 3 Gold) |
| Mock run with `ETL_GOLD_URI=s3://etl/gold` against a local moto S3 server | Gold tables created; second run merged without duplicates |
| Live API run (trial key, temp data folder) | Worked, see section 1 |

Test runs used temporary data folders, so the sample `data/` folder was not changed.

## 4. Not verified

- Real AWS S3 (only a local moto server was used; `moto`/`boto3` were installed for the test and are not in
  `requirements.txt`).
- Airflow DAG on a live Airflow instance (only syntax-checked).
- GitHub workflows and the Docker build (never run).
- Long date ranges with a paid key (`past-range`, 10-day chunks): covered only by unit/mock tests.

## 5. Still to do before handover

1. `git init`, commit and push to GitHub (the folder is not a git repo yet).
2. Remove or rotate the real API key in `.env` (gitignored, but do not copy it to the customer).
3. Pin versions in `requirements.txt` (currently `>=` ranges; local Python 3.14, CI Python 3.12).
4. Add repository secrets for the scheduled workflow.
5. Remove `req-arch/`, `__pycache__/`, `.pytest_cache/` from the customer copy.
6. Run one dated load with a paid key: `python -m electricity_etl.pipeline --start 2025-03-01T00:00:00 --end 2025-03-15T00:00:00`.
