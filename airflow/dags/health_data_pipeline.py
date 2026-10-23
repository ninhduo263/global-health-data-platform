"""Load the selected WHO COVID-19 daily dataset into the health DWH."""

import csv
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

WHO_CSV_SOURCE_PATH = os.getenv(
    "WHO_CSV_SOURCE_PATH",
    "/opt/airflow/data/raw/WHO-COVID-19-global-daily-data.csv",
)
WHO_START_DATE = date.fromisoformat(os.getenv("WHO_START_DATE", "2020-01-01"))
WHO_END_DATE = date.fromisoformat(os.getenv("WHO_END_DATE", "2023-12-31"))
WHO_COUNTRY_CODES = tuple(
    code.strip().upper()
    for code in os.getenv(
        "WHO_COUNTRY_CODES",
        "VN,US,GB,DE,FR,IT,ES,IN,CN,JP,KR,BR,RU,AU,CA",
    ).split(",")
    if code.strip()
)
STAGING_DIR = Path("/tmp/health_pipeline_staging")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "covid19-landing-zone")

DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB = os.getenv("DWH_DB", "health_dwh")
DWH_USER = os.getenv("DWH_USER", "dwh_admin")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_password")


def extract_who_csv_callable(**context) -> str:
    """Filter the WHO file by the configured period and 15 country codes."""
    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    target_csv = STAGING_DIR / f"who_covid_{execution_date}.csv"
    required = {
        "Date_reported",
        "Country_code",
        "Country",
        "WHO_region",
        "New_cases",
        "Cumulative_cases",
        "New_deaths",
        "Cumulative_deaths",
    }
    selected_rows = 0
    with open(WHO_CSV_SOURCE_PATH, encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"WHO dataset thiếu cột: {sorted(missing)}")
        with open(target_csv, "w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=sorted(required))
            writer.writeheader()
            for row in reader:
                code = (row["Country_code"] or "").strip().upper()
                report_date = date.fromisoformat(row["Date_reported"])
                if (
                    code in WHO_COUNTRY_CODES
                    and WHO_START_DATE <= report_date <= WHO_END_DATE
                ):
                    writer.writerow(row)
                    selected_rows += 1
    if not selected_rows:
        raise ValueError("Không có dòng nào khớp bộ lọc WHO")
    logger.info(
        "Filtered %d WHO rows for %d countries from %s to %s",
        selected_rows,
        len(WHO_COUNTRY_CODES),
        WHO_START_DATE,
        WHO_END_DATE,
    )
    return str(target_csv)


def load_to_minio_callable(**context) -> dict:
    """Upload the filtered WHO CSV to MinIO."""
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    ti = context["ti"]
    execution_date = context["ds"]
    who_csv_path = ti.xcom_pull(task_ids="extract_who_csv")
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
    key = f"raw/who/{execution_date}/who_covid.csv"
    s3_client.upload_file(who_csv_path, MINIO_BUCKET, key)
    return {"bucket": MINIO_BUCKET, "who_key": key}


def _integer_value(value: str | None) -> int:
    value = (value or "").strip()
    return int(float(value)) if value else 0


def transform_and_load_to_dwh_callable(**context) -> None:
    """Upsert WHO daily cases, deaths, and cumulative totals into the DWH."""
    import psycopg2

    csv_path = context["ti"].xcom_pull(task_ids="extract_who_csv")
    conn = psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dwh.fact_cases")
            cur.execute("DELETE FROM dwh.dim_country")
            cur.execute(
                """
                INSERT INTO dwh.dim_indicator (indicator_code, description)
                VALUES (%s, %s)
                ON CONFLICT (indicator_code) DO UPDATE
                    SET description = EXCLUDED.description
                RETURNING id
                """,
                (
                    "WHO_COVID_DAILY_2020_2023",
                    "WHO daily dataset, 15 countries, 2020-2023",
                ),
            )
            indicator_id = cur.fetchone()[0]
            with open(csv_path, encoding="utf-8-sig", newline="") as source:
                for row in csv.DictReader(source):
                    country_code = row["Country_code"].strip().upper()
                    report_date = date.fromisoformat(row["Date_reported"])
                    region_code = row["WHO_region"].strip().upper()
                    cur.execute(
                        """
                        INSERT INTO dwh.dim_region (continent_code, continent_name)
                        VALUES (%s, %s)
                        ON CONFLICT (continent_code) DO UPDATE
                            SET continent_name = EXCLUDED.continent_name
                        RETURNING id
                        """,
                        (region_code, region_code),
                    )
                    region_id = cur.fetchone()[0]
                    cur.execute(
                        """
                        INSERT INTO dwh.dim_country (country_code, country_name, region_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (country_code) DO UPDATE SET
                            country_name = EXCLUDED.country_name,
                            region_id = EXCLUDED.region_id,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (country_code, row["Country"], region_id),
                    )
                    country_id = cur.fetchone()[0]
                    date_id = int(report_date.strftime("%Y%m%d"))
                    cur.execute(
                        """
                        INSERT INTO dwh.dim_date
                            (date_id, full_date, year, month, day, is_weekend)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date_id) DO NOTHING
                        """,
                        (
                            date_id,
                            report_date,
                            report_date.year,
                            report_date.month,
                            report_date.day,
                            report_date.isoweekday() in (6, 7),
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO dwh.fact_cases
                            (country_id, date_id, indicator_id, new_cases,
                             new_deaths, total_cases, total_deaths)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_id, date_id) DO UPDATE SET
                            indicator_id = EXCLUDED.indicator_id,
                            new_cases = EXCLUDED.new_cases,
                            new_deaths = EXCLUDED.new_deaths,
                            total_cases = EXCLUDED.total_cases,
                            total_deaths = EXCLUDED.total_deaths
                        """,
                        (
                            country_id,
                            date_id,
                            indicator_id,
                            _integer_value(row["New_cases"]),
                            _integer_value(row["New_deaths"]),
                            _integer_value(row["Cumulative_cases"]),
                            _integer_value(row["Cumulative_deaths"]),
                        ),
                    )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("WHO dataset load failed")
        raise
    finally:
        conn.close()


default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="health_data_pipeline",
    default_args=default_args,
    description="Load the selected WHO COVID-19 daily dataset into PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["public_health", "who", "snowflake_dwh"],
) as dag:
    extract_who_csv = PythonOperator(
        task_id="extract_who_csv",
        python_callable=extract_who_csv_callable,
    )
    load_to_minio = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio_callable,
    )
    transform_and_load_to_dwh = PythonOperator(
        task_id="transform_and_load_to_dwh",
        python_callable=transform_and_load_to_dwh_callable,
    )
    extract_who_csv >> load_to_minio >> transform_and_load_to_dwh
