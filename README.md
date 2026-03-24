# Retail Analytics Data Pipeline (Airflow + Postgres)

## Description

A reproducible retail analytics pipeline built with Apache Airflow and PostgreSQL. It ingests e-commerce CSV data, builds layered SQL models (`raw -> stg -> core -> mart`), and validates quality with SQL tests.

## Tech Stack

| Component                                            | Purpose                                                                                                    |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Docker + Docker Compose                              | Spins up the local environment and connects all project services                                           |
| Apache Airflow                                       | Orchestrates the batch pipeline: data preparation, raw load, SQL transformations, and data quality checks |
| PostgreSQL (`airflow` DB)                            | Airflow metadata database: DAG runs, task states, scheduler metadata                                       |
| PostgreSQL (`retail` DB)                             | Analytical warehouse for the project: `raw`, `stg`, `core`, and `mart` layers                             |
| Python 3.8                                           | Used for dataset preparation and loading raw CSV files into PostgreSQL                                     |
| CSV dataset (Olist / sample files)                   | Source input data for the retail pipeline                                                                  |
| SQL (`staging`)                                      | Cleans and standardizes raw data: type casting, trimming, null handling, basic normalization              |
| SQL (`core`)                                         | Builds analytics-ready core tables: customer/product dimensions and item-level fact table                 |
| SQL (`mart`)                                         | Builds business-facing marts such as daily sales KPIs                                                      |
| SQL tests                                            | Validates data quality: not-null checks, uniqueness, referential integrity, value constraints             |
| Volume mounts (`dags/`, `sql/`, `scripts/`, `data/`) | Makes DAGs, SQL models, scripts, and local datasets available inside Airflow containers                   |

## Project Structure

- `dags/` - Airflow DAG `retail_pipeline_daily` and pipeline step orchestration.
- `scripts/` - Python scripts for dataset download and raw layer loading.
- `sql/staging/` - SQL models for the `stg` layer (data cleaning and type casting).
- `sql/core/` - SQL models for the `core` layer (main analytics-ready tables).
- `sql/marts/` - SQL marts with business metrics (for example, `mart.daily_sales`).
- `sql/tests/` - SQL data quality checks and a fail-fast test runner.
- `postgres/init/` - Init scripts for creating the `retail` database and schemas.
- `data/` - Input CSV files (`full` and `sample`) loaded into `raw`.

## Architecture

The pipeline is implemented as a daily batch process in Airflow with layered data modeling in PostgreSQL.

##### <span style="text-decoration: underline;">Data Source</span>

- The full dataset is downloaded from Kaggle: `olistbr/brazilian-ecommerce`.
- The `scripts/download_dataset.py` script:
  - downloads full CSV files into `data/full/` (if they are missing),
  - creates a sample subset in `data/sample/` for fast local testing.

##### <span style="text-decoration: underline;">Databases and Schemas</span>

A single PostgreSQL instance contains two databases:

- `airflow` - Airflow metadata,
- `retail` - pipeline warehouse.

The `retail` database uses these schemas:

- `raw` - raw layer immediately after CSV load,
- `stg` - cleaning and type casting (views),
- `core` - business-ready tables,
- `mart` - analytical marts.

##### <span style="text-decoration: underline;">DAG Orchestration</span>

DAG: `retail_pipeline_daily`

Task order:

1. `generate_data` - prepare full/sample CSV files.
2. `load_raw` - load CSV files into `raw` via `COPY`.
3. `run_staging_sql` - execute `sql/staging/*.sql`.
4. `run_core_sql` - execute `sql/core/*.sql`.
5. `run_marts_sql` - execute `sql/marts/*.sql`.
6. `run_sql_tests` - execute SQL tests.

##### <span style="text-decoration: underline;">Data Quality Control</span>

- `sql/tests/tests.sql` - outputs per-test results (`test_name`, `failed_rows`).
- `sql/tests/run_tests.sql` - fail-fast runner: raises an exception if violations exist.

The DAG is considered successful only when ingestion, transformations, and tests complete without errors.

##### <span style="text-decoration: underline;">mart.daily_sales Mart</span>

The `mart.daily_sales` mart aggregates daily sales and includes key KPIs:

- `order_date` - sales date.
- `total_orders` - number of unique orders per day.
- `total_revenue` - total daily revenue (sum of `price`).
- `aov` - average order value (`total_revenue / total_orders`).
- `unique_customers` - number of unique customers per day.

Only orders with statuses `delivered` and `shipped` are included.

## Local Run

1. Start services:

```bash
docker compose up -d
```

2. Open Airflow UI and run the `retail_pipeline_daily` DAG.

3. Validate output in `mart.daily_sales`.

## Notes

- `data/full/` and `logs/` are not committed to git.
- `data/sample/` can be kept in the repository for reproducible demo runs without Kaggle access.
- Downloading full data requires Kaggle API access in the container environment.
