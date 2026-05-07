"""
dags/mmm_pipeline.py
Orchestrates: generate data → load → dbt staging → dbt marts
Schedule: daily at 6am UTC
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner":            "deep",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}


@dag(
    dag_id="mmm_pipeline",
    description="Ad spend MMM pipeline: generate → load → dbt",
    schedule="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mmm", "ad-spend", "dbt"],
)
def mmm_pipeline():

    @task()
    def generate_google():
        result = subprocess.run(
            [sys.executable, "/opt/airflow/extractors/01_generate_google.py"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Google generate failed:\n{result.stderr}")
        print(result.stdout)

    @task()
    def generate_meta():
        result = subprocess.run(
            [sys.executable, "/opt/airflow/extractors/02_generate_meta.py"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Meta generate failed:\n{result.stderr}")
        print(result.stdout)

    @task()
    def generate_tiktok():
        result = subprocess.run(
            [sys.executable, "/opt/airflow/extractors/03_generate_tiktok.py"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"TikTok generate failed:\n{result.stderr}")
        print(result.stdout)

    @task()
    def generate_reddit():
        result = subprocess.run(
            [sys.executable, "/opt/airflow/extractors/04_generate_reddit.py"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Reddit generate failed:\n{result.stderr}")
        print(result.stdout)

    @task()
    def load_to_postgres():
        """Load all 4 CSVs into Postgres raw schema using psycopg2 COPY."""
        import psycopg2

        user = os.environ["POSTGRES_USER"]
        pw   = os.environ["POSTGRES_PASSWORD"]
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        db   = os.environ["POSTGRES_DB"]

        conn = psycopg2.connect(
            host=host, port=int(port), dbname=db, user=user, password=pw
        )
        cur = conn.cursor()

        data_dir = "/opt/airflow/data/raw"
        platforms = {
            "google_ads_raw": "google_ads.csv",
            "meta_ads_raw":   "meta_ads.csv",
            "tiktok_ads_raw": "tiktok_ads.csv",
            "reddit_ads_raw": "reddit_ads.csv",
        }

        for table, filename in platforms.items():
            path = os.path.join(data_dir, filename)

            with open(path, "r") as f:
                headers = next(csv.reader(f))

            cur.execute(f"DROP TABLE IF EXISTS raw.{table} CASCADE")
            cols_ddl = ", ".join([f'"{h}" TEXT' for h in headers])
            cur.execute(f"CREATE TABLE raw.{table} ({cols_ddl})")

            with open(path, "r") as f:
                cur.copy_expert(f'COPY raw.{table} FROM STDIN WITH CSV HEADER', f)

            conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM raw.{table}")
            count = cur.fetchone()[0]
            print(f"  Loaded raw.{table}: {count:,} rows")

        cur.close()
        conn.close()
        print("✅ All raw tables loaded")

    @task()
    def run_dbt_staging():
        """Run dbt staging models."""
        result = subprocess.run(
            [
                "/home/airflow/.local/bin/dbt",
                "run",
                "--select", "staging",
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", "/opt/airflow/dbt",
            ],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"dbt staging failed:\n{result.stderr}")

    @task()
    def run_dbt_marts():
        """Run dbt mart model."""
        result = subprocess.run(
            [
                "/home/airflow/.local/bin/dbt",
                "run",
                "--select", "marts",
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", "/opt/airflow/dbt",
            ],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"dbt marts failed:\n{result.stderr}")

    g = generate_google()
    m = generate_meta()
    t = generate_tiktok()
    r = generate_reddit()
    load = load_to_postgres()
    staging = run_dbt_staging()
    marts = run_dbt_marts()

    [g, m, t, r] >> load >> staging >> marts


mmm_pipeline()