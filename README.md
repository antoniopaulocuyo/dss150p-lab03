# DSS150P — Laboratory Activity 3: Productionizing a Modular Data Pipeline

A reproducible, modular, rerun-safe, benchmarked, partitioned, and
orchestrated data pipeline for e-commerce sales-order-line analytics,
built across four goals: environment/config, ETL, storage/benchmarking,
and Airflow orchestration.

## Project structure

src/
├── config.py           # Merges config/settings.yml + .env into SETTINGS/DB
├── common/
│   ├── audit.py         # utc_now_iso, new_run_id, record_hash helpers
│   └── errors.py        # PipelineStageError (stage/run_id-tagged exceptions)
├── extract/
│   └── files.py         # Raw source snapshotting (data/raw/run_id=...)
├── transform/
│   ├── staging.py       # Dedup, typing, quality-rule quarantine
│   └── curated.py       # Cross-source join, money calcs, record_hash
├── load/
│   └── postgres.py      # Rerun-safe UPSERT, partition load, run audit
├── validate/
│   └── quality.py       # Post-load data contract checks
├── benchmark/
│   └── storage.py       # CSV/JSONL/Parquet/Postgres comparison + partitioning
└── cli.py               # Thin CLI wiring all of the above

dags/
└── dss150p_pipeline.py  # Airflow DAG (calls src.cli only, no business logic)

evidence/                # Screenshots + written findings per goal/task

## 1. Environment setup

**Requires Python 3.11.** Newer interpreters (3.13+) don't have prebuilt
wheels for the pinned `pandas==2.2.3`/`pyarrow==17.0.0` and will fail to
build from source — if your system's default `python3` is newer, install
3.11 explicitly (e.g. `brew install python@3.11` on macOS) and use it below.

```bash
python3.11 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\Activate.ps1     # Windows PowerShell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Copy the environment template and set a real local password:
```bash
cp .env.example .env
# edit .env, set POSTGRES_PASSWORD to something other than change_me
```

Verify the environment and configuration wiring:
```bash
python -m src.cli validate-env
```

## 2. Docker / PostgreSQL

```bash
docker compose build pipeline
docker compose up -d postgres
docker compose ps
docker compose run --rm pipeline python -m src.cli validate-env
```

Verify schema initialization:
```bash
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dt curated.*"
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dt audit.*"
```
Expected schemas: `staging`, `curated`, `audit`. Expected tables:
`curated.sales_order_lines`, `audit.pipeline_runs`, `audit.partition_loads`.

## 3. Running the pipeline (Goal 2)

```bash
python -m src.cli run-all       # extract -> staging -> curated -> load -> validate
python -m src.cli load          # rerun-safe standalone load (reads last curated snapshot)
python -m src.cli load          # run again to prove no duplicates are created
```

Verify rerun-safety:
```bash
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c \
  "SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines;"
```
`total` should equal `distinct_orders` regardless of how many times `load`
is run.

Individual stages can also be run on their own:
```bash
python -m src.cli extract
python -m src.cli transform
python -m src.cli validate
```

## 4. Storage benchmarking and partitioning (Goal 3)

```bash
python -m src.cli benchmark --repeats 5
```
Writes `data/benchmarks/{sales_order_lines.csv,.jsonl,.parquet,benchmark_results.csv}`
and prints a size/write-time/read-time comparison across CSV, JSON Lines,
Parquet (Snappy), and PostgreSQL. See `evidence/9_Task_Analysis.md` for the
full interpretation and answers to the Goal 3 analysis questions.

Partitioned Parquet (by `order_year`/`order_month`) is written as part of
the same benchmark run's dependency chain — see `src/benchmark/storage.py:write_partitioned_parquet`.
Load a single partition into PostgreSQL:
```bash
python -m src.cli load-partition --year 2026 --month 1
python -m src.cli load-partition --year 2026 --month 1   # run again to confirm dedup
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT * FROM audit.partition_loads;"
```

**Note on PostgreSQL storage size:** because `upsert_curated`/`load_partition`
unconditionally advance `pipeline_run_id`/`processed_at_utc` on every row on
every load (to keep the audit trail accurate — see Technical Question 1's
discussion of `record_hash` exclusions), repeated loads generate MVCC dead
tuples that `pg_total_relation_size` includes until autovacuum (or a manual
`VACUUM FULL curated.sales_order_lines;`) reclaims them. This is expected
behavior, not a leak — worth running a manual `VACUUM` before a benchmark run
if you want a clean size comparison.

## 5. Airflow orchestration (Goal 4)

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps
```

Airflow UI: **http://localhost:8080** — training credentials `admin`/`admin`
(local lab use only).

The DAG (`dss150p_sales_pipeline`) runs daily at `0 2 * * *` UTC, with
`catchup=False` (each run reflects the *current* source snapshot, not a
date-partitioned slice — see `dags/dss150p_pipeline.py` comments and
`evidence/10.6_Optional.md` for the full backfill-reasoning rationale).

Parameters (`run_mode`, `year`, `month`) are exposed via **Trigger DAG w/
config**:
- `run_mode=full` → `extract → transform → load → validate` (full curated reload)
- `run_mode=partition` → same chain, but `load` invokes
  `python -m src.cli load-partition --year <year> --month <month>` instead

All four tasks in a single DAG run share one `pipeline_run_id`, passed via
the `PIPELINE_RUN_ID` environment variable (Airflow's `{{ run_id }}`) and
read by `src/cli.py`'s `main()` — manual CLI invocations without that env
var still generate their own fresh run ID via `new_run_id()`.

Retries: 2, with a 1-minute delay; `execution_timeout` of 10 minutes per
task; `on_failure_callback` prints stage/run/error context to the task log
on failure.

## Evidence

All screenshots and written findings are in `evidence/`, organized by
spec section number (e.g. `7.1_Task_A.png` = section 7.1 Task A,
`10.5_Task_E_Failure.png` = section 10.5 Task E). Written analysis:

- `evidence/8.2_Task_A_Raw_Extraction_Insights.md`, `8.3_Task_B_Staging_Results.md`,
  `Task8.4_Curated_Transformation.md` — Goal 2 profiling findings and staging/curated results
- `evidence/9_Task_Analysis.md` — Goal 3 storage benchmark interpretation and analysis-question answers
- `evidence/10.6_Optional.md` — Goal 4 backfill reasoning
- `evidence/Technical_Questions.md` — section 15 answers

## Git history

Work is organized by goal, one branch each, merged into `main` on
completion: `goal1-reproducible-environment`, `goal2-etl-pipeline`,
`goal3-storage-formats`, `goal4-airflow-orchestration`.