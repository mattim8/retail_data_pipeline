from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


SCRIPTS_DIR = Path("/opt/airflow/scripts")
SQL_BASE_DIR = Path("/opt/airflow/sql")


def _run_python_script(script_name: str) -> None:
    script_path = SCRIPTS_DIR / script_name
    result = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Script failed: {script_name}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )


def _get_connection():
    return psycopg2.connect(
        dbname=os.environ["RETAIL_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
    )


def _execute_sql_directory(sql_subdir: str) -> None:
    sql_dir = SQL_BASE_DIR / sql_subdir
    sql_files = sorted(sql_dir.glob("*.sql"))

    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for sql_file in sql_files:
                    sql_text = sql_file.read_text(encoding="utf-8")
                    cur.execute(sql_text)
    finally:
        conn.close()


def _run_sql_tests() -> None:
    sql_dir = SQL_BASE_DIR / "tests"
    sql_files = sorted(sql_dir.glob("*.sql"))

    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for sql_file in sql_files:
                    sql_text = sql_file.read_text(encoding="utf-8")
                    cur.execute(sql_text)

                    if cur.description is None:
                        continue

                    rows = cur.fetchall()
                    if not rows:
                        continue

                    columns = [desc[0].lower() for desc in cur.description]
                    if "failed_rows" in columns:
                        failed_idx = columns.index("failed_rows")
                        failed_total = sum(int(row[failed_idx]) for row in rows)
                        if failed_total > 0:
                            raise RuntimeError(
                                f"SQL tests failed in {sql_file.name}: {failed_total} violations"
                            )
                    else:
                        raise RuntimeError(
                            f"SQL tests returned rows in {sql_file.name}; expected zero rows."
                        )
    finally:
        conn.close()


with DAG(
    dag_id="retail_pipeline_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["retail", "pipeline"],
) as dag:
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=_run_python_script,
        op_kwargs={"script_name": "download_dataset.py"},
    )

    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=_run_python_script,
        op_kwargs={"script_name": "load_raw.py"},
    )

    run_staging_sql = PythonOperator(
        task_id="run_staging_sql",
        python_callable=_execute_sql_directory,
        op_kwargs={"sql_subdir": "staging"},
    )

    run_core_sql = PythonOperator(
        task_id="run_core_sql",
        python_callable=_execute_sql_directory,
        op_kwargs={"sql_subdir": "core"},
    )

    run_marts_sql = PythonOperator(
        task_id="run_marts_sql",
        python_callable=_execute_sql_directory,
        op_kwargs={"sql_subdir": "marts"},
    )

    run_sql_tests = PythonOperator(
        task_id="run_sql_tests",
        python_callable=_run_sql_tests,
    )

    generate_data >> load_raw >> run_staging_sql >> run_core_sql >> run_marts_sql >> run_sql_tests
