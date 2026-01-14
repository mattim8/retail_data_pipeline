# Retail Analytics Data Pipeline (Airflow + Postgres)


## Description

A reproducible retail analytics pipeline built with Apache Airflow and PostgreSQL. It generates and ingests e-commerce data, builds layered SQL models (raw - staging - core - mart), and enforces data quality via SQL checks.

## Tech stack:

- **Orchestration: Apache Airflow**
- **Storage: PostgreSQL**
- **Transforms: SQL (layered modeling: raw/staging/core/mart)**
- **Ingestion: Python (CSV generation + COPY load)**
- **Data quality: SQL checks (fail the DAG on violations)**
- **Infrastructure: Docker Compose**