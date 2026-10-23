"""Load the selected WHO COVID-19 daily dataset into the health DWH."""

import csv
import logging
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# ============================================================================
# CẤU HÌNH ENDPOINT LIVE API & KẾT NỐI HỆ THỐNG
# ============================================================================
OWID_API_URL = os.getenv(
    "OWID_API_URL",
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
)
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
OLTP_HOST = os.getenv("OLTP_HOST", "mysql-oltp")
OLTP_PORT = int(os.getenv("OLTP_PORT", "3306"))
OLTP_DB = os.getenv("OLTP_DB", "vaccination_oltp")
OLTP_USER = os.getenv("OLTP_USER", "oltp_user")
OLTP_PASSWORD = os.getenv("OLTP_PASSWORD", "oltp_password")

# ============================================================================
# CÁC HÀM XỬ LÝ CHO TỪNG TASK (100% LIVE API & DATABASE QUERIES)
# ============================================================================

def extract_owid_api_callable(**context) -> str:
    """
    Download OWID's historical global COVID-19 dataset as a raw CSV snapshot.
    """
    import csv

    import requests

    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    output_file = STAGING_DIR / f"owid_covid_{execution_date}.csv"

    logger.info("Tải dữ liệu COVID-19 lịch sử toàn cầu từ OWID: %s", OWID_API_URL)
    with requests.get(
        OWID_API_URL,
        stream=True,
        timeout=(15, 300),
        headers={"User-Agent": "global-health-data-platform/1.0"},
    ) as response:
        response.raise_for_status()
        with output_file.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)

    required_columns = {
        "iso_code",
        "continent",
        "location",
        "date",
        "total_cases",
        "new_cases",
        "total_deaths",
        "new_deaths",
        "total_vaccinations",
        "new_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "population",
        "gdp_per_capita",
        "hospital_beds_per_thousand",
    }
    with output_file.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "OWID CSV is missing required columns: "
                + ", ".join(sorted(missing_columns))
            )
        first_row = next(reader, None)
        if first_row is None:
            raise ValueError("OWID CSV contains a header but no data rows")

    logger.info("Đã tải dữ liệu OWID CSV toàn cầu tại: %s", output_file)
    return str(output_file)

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


def extract_mysql_oltp_callable(**context) -> dict:
    """Export the OLTP vaccination tables as raw CSV files for the MinIO landing zone."""
    import csv
    import pymysql
    from pymysql.cursors import DictCursor

    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    conn = pymysql.connect(
        host=OLTP_HOST,
        port=OLTP_PORT,
        user=OLTP_USER,
        password=OLTP_PASSWORD,
        database=OLTP_DB,
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )
    output_paths = {}
    try:
        with conn.cursor() as cursor:
            for table_name, columns in {
                "vaccination_centers": (
                    "center_id, center_code, center_name, province, country_code, created_at"
                ),
                "vaccination_records": (
                    "record_id, patient_id, citizen_hash, center_id, country_code, "
                    "vaccine_type, dose_number, batch_number, vaccination_date, created_at, updated_at"
                ),
            }.items():
                cursor.execute(f"SELECT {columns} FROM `{table_name}`")
                rows = cursor.fetchall()
                output_file = STAGING_DIR / f"{table_name}_{execution_date}.csv"
                fieldnames = columns.replace(" ", "").split(",")
                with output_file.open("w", encoding="utf-8", newline="") as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                output_paths[table_name] = str(output_file)
                logger.info(
                    "Đã trích xuất %s bản ghi từ MySQL.%s vào %s",
                    len(rows),
                    table_name,
                    output_file,
                )
    finally:
        conn.close()

    return output_paths


def load_to_minio_callable(**context) -> dict:
    """Upload OWID, WHO, and OLTP snapshots to MinIO."""
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    ti = context["ti"]
    execution_date = context["ds"]
    owid_csv_path = ti.xcom_pull(task_ids="extract_owid_api")
    who_csv_path = ti.xcom_pull(task_ids="extract_who_csv")
    mysql_oltp_paths = ti.xcom_pull(task_ids="extract_mysql_oltp")
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
    # Phân vùng thư mục trên Data Lake theo ngày (Partitioning by date)
    owid_s3_key = f"raw/owid/{execution_date}/owid_covid.csv"
    who_s3_key = f"raw/who/{execution_date}/who_covid.csv"

    s3_client.upload_file(owid_csv_path, MINIO_BUCKET, owid_s3_key)
    logger.info("Đã upload thành công: s3://%s/%s", MINIO_BUCKET, owid_s3_key)

    s3_client.upload_file(who_csv_path, MINIO_BUCKET, who_s3_key)
    logger.info("Đã upload thành công: s3://%s/%s", MINIO_BUCKET, who_s3_key)

    mysql_s3_keys = {}
    for table_name, file_path in mysql_oltp_paths.items():
        object_key = f"raw/oltp/{execution_date}/{table_name}.csv"
        s3_client.upload_file(file_path, MINIO_BUCKET, object_key)
        mysql_s3_keys[table_name] = object_key
        logger.info("Đã upload thành công: s3://%s/%s", MINIO_BUCKET, object_key)

    return {
        "bucket": MINIO_BUCKET,
        "owid_key": owid_s3_key,
        "who_key": who_s3_key,
        "oltp_keys": mysql_s3_keys,
    }


def transform_and_load_to_dwh_callable(**context) -> None:
    """
    Load OWID cases and the selected WHO dataset from MinIO into separate DWH facts.
    """
    import boto3
    import csv

    from botocore.client import Config
    import psycopg2
    from psycopg2.extras import execute_values

    ti = context["ti"]
    minio_meta = ti.xcom_pull(task_ids="load_to_minio")
    if not minio_meta:
        raise ValueError("MinIO object metadata was not found")

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_date = context["ds_nodash"]
    owid_csv_path = STAGING_DIR / f"owid_from_minio_{snapshot_date}.csv"
    who_csv_path = STAGING_DIR / f"who_from_minio_{snapshot_date}.csv"
    s3_client.download_file(minio_meta["bucket"], minio_meta["owid_key"], str(owid_csv_path))
    s3_client.download_file(minio_meta["bucket"], minio_meta["who_key"], str(who_csv_path))
    continent_codes = {
        "Africa": "AF",
        "Asia": "AS",
        "Europe": "EU",
        "North America": "NA",
        "Oceania": "OC",
        "South America": "SA",
    }

    def integer_value(value: str) -> int:
        """Convert missing, negative, or malformed counts to zero."""
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        if not math.isfinite(number) or number < 0:
            return 0
        return int(number)

    def signed_integer_value(value: str) -> int:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        return int(number) if math.isfinite(number) else 0

    def nullable_integer(value: str):
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return int(number) if math.isfinite(number) and number >= 0 else None

    def nullable_float(value: str):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) and number >= 0 else None

    conn = psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dwh.fact_who_covid_daily")
            cur.execute("DELETE FROM dwh.fact_cases")
            cur.execute("DELETE FROM dwh.dim_country")
            cur.execute(
                """
                ALTER TABLE dwh.fact_cases
                    ADD COLUMN IF NOT EXISTS new_vaccinations BIGINT NOT NULL DEFAULT 0
                        CHECK (new_vaccinations >= 0),
                    ADD COLUMN IF NOT EXISTS people_vaccinated BIGINT NOT NULL DEFAULT 0
                        CHECK (people_vaccinated >= 0),
                    ADD COLUMN IF NOT EXISTS people_fully_vaccinated BIGINT NOT NULL DEFAULT 0
                        CHECK (people_fully_vaccinated >= 0),
                    ADD COLUMN IF NOT EXISTS total_cases BIGINT NOT NULL DEFAULT 0
                        CHECK (total_cases >= 0),
                    ADD COLUMN IF NOT EXISTS total_deaths BIGINT NOT NULL DEFAULT 0
                        CHECK (total_deaths >= 0);

                CREATE OR REPLACE VIEW dwh.vw_dashboard_cases AS
                SELECT
                    d.full_date AS report_date,
                    d.year,
                    d.month,
                    c.country_code,
                    c.country_name,
                    r.continent_name,
                    f.new_cases,
                    f.new_deaths,
                    f.total_vaccinations,
                    f.total_cases,
                    f.total_deaths,
                    f.new_vaccinations,
                    f.people_vaccinated,
                    f.people_fully_vaccinated
                FROM dwh.fact_cases AS f
                JOIN dwh.dim_country AS c ON c.id = f.country_id
                JOIN dwh.dim_date AS d ON d.date_id = f.date_id
                LEFT JOIN dwh.dim_region AS r ON r.id = c.region_id;
                """
            )

            cur.execute(
                """
                INSERT INTO dwh.dim_indicator (indicator_code, description)
                VALUES (%s, %s)
                ON CONFLICT (indicator_code) DO UPDATE
                    SET description = EXCLUDED.description
                RETURNING id
                """,
                (
                    "OWID_COVID_DAILY_GLOBAL",
                    "Global country-level COVID-19 observations from OWID",
                ),
            )
            indicator_id = cur.fetchone()[0]

            who_rows = []
            with open(who_csv_path, mode="r", encoding="utf-8-sig", newline="") as source:
                for row in csv.DictReader(source):
                    country_code = (row.get("Country_code") or "").strip().upper()
                    if len(country_code) != 2:
                        continue
                    who_rows.append(
                        (
                            date.fromisoformat(row["Date_reported"]),
                            country_code,
                            row["Country"],
                            row["WHO_region"],
                            signed_integer_value(row.get("New_cases")),
                            integer_value(row.get("Cumulative_cases")),
                            signed_integer_value(row.get("New_deaths")),
                            integer_value(row.get("Cumulative_deaths")),
                        )
                    )
            if who_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_who_covid_daily (
                        report_date, country_code, country_name, who_region,
                        new_cases, cumulative_cases, new_deaths, cumulative_deaths
                    ) VALUES %s
                    ON CONFLICT (report_date, country_code) DO UPDATE SET
                        country_name = EXCLUDED.country_name,
                        who_region = EXCLUDED.who_region,
                        new_cases = EXCLUDED.new_cases,
                        cumulative_cases = EXCLUDED.cumulative_cases,
                        new_deaths = EXCLUDED.new_deaths,
                        cumulative_deaths = EXCLUDED.cumulative_deaths,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    who_rows,
                    page_size=1000,
                )
                logger.info("Loaded %s WHO observations into the separate WHO fact", len(who_rows))

            country_ids = {}
            region_ids = {}
            date_ids = set()
            fact_rows = []
            loaded_rows = 0

            def flush_facts():
                if not fact_rows:
                    return
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_cases (
                        country_id, date_id, indicator_id, new_cases, new_deaths, new_tests,
                        total_vaccinations, new_vaccinations, people_vaccinated,
                        people_fully_vaccinated, total_cases, total_deaths
                    ) VALUES %s
                    ON CONFLICT (country_id, date_id) DO UPDATE SET
                        indicator_id = EXCLUDED.indicator_id,
                        new_cases = EXCLUDED.new_cases,
                        new_deaths = EXCLUDED.new_deaths,
                        new_tests = EXCLUDED.new_tests,
                        total_vaccinations = EXCLUDED.total_vaccinations,
                        new_vaccinations = EXCLUDED.new_vaccinations,
                        people_vaccinated = EXCLUDED.people_vaccinated,
                        people_fully_vaccinated = EXCLUDED.people_fully_vaccinated,
                        total_cases = EXCLUDED.total_cases,
                        total_deaths = EXCLUDED.total_deaths;
                    """,
                    fact_rows,
                    page_size=1000,
                )
                fact_rows.clear()

            with open(owid_csv_path, mode="r", encoding="utf-8-sig", newline="") as source:
                reader = csv.DictReader(source)
                for record in reader:
                    country_code = (record.get("iso_code") or "").strip()
                    continent = (record.get("continent") or "").strip()
                    country_name = (record.get("location") or "").strip()
                    if (
                        len(country_code) != 3
                        or not country_code.isalpha()
                        or not continent
                        or not country_name
                    ):
                        continue

                    if country_code not in country_ids:
                        if continent not in region_ids:
                            region_code = continent_codes.get(continent)
                            if region_code is None:
                                logger.warning("Bỏ qua châu lục OWID chưa ánh xạ: %s", continent)
                                continue
                            cur.execute(
                                """
                                INSERT INTO dwh.dim_region (continent_code, continent_name)
                                VALUES (%s, %s)
                                ON CONFLICT (continent_code) DO UPDATE
                                    SET continent_name = EXCLUDED.continent_name
                                RETURNING id;
                                """,
                                (region_code, continent),
                            )
                            region_ids[continent] = cur.fetchone()[0]

                        cur.execute(
                            """
                            INSERT INTO dwh.dim_country (
                                country_code, country_name, population, gdp, beds_per_1000,
                                region_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (country_code) DO UPDATE SET
                                country_name = EXCLUDED.country_name,
                                population = COALESCE(EXCLUDED.population, dwh.dim_country.population),
                                gdp = COALESCE(EXCLUDED.gdp, dwh.dim_country.gdp),
                                beds_per_1000 = COALESCE(
                                    EXCLUDED.beds_per_1000, dwh.dim_country.beds_per_1000
                                ),
                                region_id = EXCLUDED.region_id,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING id;
                            """,
                            (
                                country_code,
                                country_name,
                                nullable_integer(record.get("population")),
                                nullable_float(record.get("gdp_per_capita")),
                                nullable_float(record.get("hospital_beds_per_thousand")),
                                region_ids[continent],
                            ),
                        )
                        country_ids[country_code] = cur.fetchone()[0]

                    date_value = (record.get("date") or "").strip()
                    if not date_value:
                        continue
                    dt = datetime.strptime(date_value, "%Y-%m-%d").date()
                    date_id = int(dt.strftime("%Y%m%d"))
                    if date_id not in date_ids:
                        cur.execute(
                            """
                            INSERT INTO dwh.dim_date (
                                date_id, full_date, year, month, day, is_weekend
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (date_id) DO NOTHING;
                            """,
                            (date_id, dt, dt.year, dt.month, dt.day, dt.isoweekday() in (6, 7)),
                        )
                        date_ids.add(date_id)

                    fact_rows.append(
                        (
                            country_ids[country_code],
                            date_id,
                            indicator_id,
                            integer_value(record.get("new_cases")),
                            integer_value(record.get("new_deaths")),
                            integer_value(record.get("new_tests")),
                            integer_value(record.get("total_vaccinations")),
                            integer_value(record.get("new_vaccinations")),
                            integer_value(record.get("people_vaccinated")),
                            integer_value(record.get("people_fully_vaccinated")),
                            integer_value(record.get("total_cases")),
                            integer_value(record.get("total_deaths")),
                        )
                    )
                    loaded_rows += 1
                    if len(fact_rows) >= 5000:
                        flush_facts()

            flush_facts()
            if loaded_rows == 0:
                raise ValueError("OWID CSV contains no valid country-level observations")

        conn.commit()
        logger.info(
            "Đã nạp %s quan sát OWID cho %s quốc gia và %s ngày vào DWH.",
            loaded_rows,
            len(country_ids),
            len(date_ids),
        )
    except Exception:
        conn.rollback()
        logger.exception("DWH data load failed")
        raise
    finally:
        conn.close()


def cluster_weekly_covid_callable(**context) -> None:
    """Cluster weekly country-level COVID-19 observations and persist them to DWH."""
    import psycopg2
    from psycopg2.extras import execute_values

    from weekly_covid_kmeans import cluster_weekly_records

    conn = psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH weekly AS (
                    SELECT
                        c.country_code,
                        c.country_name,
                        c.population,
                        date_trunc('week', d.full_date)::date AS week_start,
                        SUM(f.new_cases)::bigint AS weekly_cases,
                        SUM(f.new_deaths)::bigint AS weekly_deaths
                    FROM dwh.fact_cases AS f
                    JOIN dwh.dim_country AS c ON c.id = f.country_id
                    JOIN dwh.dim_date AS d ON d.date_id = f.date_id
                    WHERE c.population > 0
                    GROUP BY c.country_code, c.country_name, c.population,
                             date_trunc('week', d.full_date)::date
                ),
                with_previous_week AS (
                    SELECT
                        weekly.*,
                        LAG(weekly_cases) OVER (
                            PARTITION BY country_code ORDER BY week_start
                        ) AS previous_week_cases
                    FROM weekly
                )
                SELECT
                    week_start,
                    country_code,
                    country_name,
                    population,
                    weekly_cases,
                    weekly_deaths,
                    previous_week_cases
                FROM with_previous_week
                ORDER BY week_start, country_code
                """
            )
            columns = (
                "week_start",
                "country_code",
                "country_name",
                "population",
                "weekly_cases",
                "weekly_deaths",
                "previous_week_cases",
            )
            weekly_records = [dict(zip(columns, row)) for row in cur.fetchall()]
            if not weekly_records:
                raise ValueError("No weekly country-level COVID-19 records found in DWH")

            clustered_records = cluster_weekly_records(
                weekly_records,
                requested_clusters=int(os.getenv("COVID_KMEANS_CLUSTERS", "4")),
            )
            cluster_count = len({record["cluster_id"] for record in clustered_records})
            values = [
                (
                    record["week_start"],
                    record["country_code"],
                    record["country_name"],
                    record["cluster_id"],
                    cluster_count,
                    record["weekly_cases"],
                    record["weekly_deaths"],
                    record["population"],
                    record["cases_per_100k"],
                    record["deaths_per_100k"],
                    record["case_growth_pct"],
                    "weekly-kmeans-v1",
                )
                for record in clustered_records
            ]
            execute_values(
                cur,
                """
                INSERT INTO dwh.fact_weekly_covid_cluster (
                    week_start, country_code, country_name, cluster_id, cluster_count,
                    weekly_cases, weekly_deaths, population, cases_per_100k,
                    deaths_per_100k, case_growth_pct, model_version
                ) VALUES %s
                ON CONFLICT (week_start, country_code, model_version) DO UPDATE SET
                    country_name = EXCLUDED.country_name,
                    cluster_id = EXCLUDED.cluster_id,
                    cluster_count = EXCLUDED.cluster_count,
                    weekly_cases = EXCLUDED.weekly_cases,
                    weekly_deaths = EXCLUDED.weekly_deaths,
                    population = EXCLUDED.population,
                    cases_per_100k = EXCLUDED.cases_per_100k,
                    deaths_per_100k = EXCLUDED.deaths_per_100k,
                    case_growth_pct = EXCLUDED.case_growth_pct,
                    computed_at = CURRENT_TIMESTAMP
                """,
                values,
                page_size=1000,
            )
        conn.commit()
        logger.info(
            "Persisted %s weekly country observations into %s K-means clusters",
            len(clustered_records),
            cluster_count,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================================
# KHỞI TẠO ĐỐI TƯỢNG DAG
# ============================================================================
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
    description=(
        "Load OWID COVID-19 history into the DWH, retain selected WHO source data "
        "in MinIO, and cluster weekly country trends."
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["public_health", "who", "owid", "kmeans", "snowflake_dwh"],
) as dag:

    # Task 1: Tải snapshot CSV COVID-19 toàn cầu từ Our World in Data
    extract_owid_api = PythonOperator(
        task_id="extract_owid_api",
        python_callable=extract_owid_api_callable,
        doc_md="Tải dữ liệu COVID-19 lịch sử toàn cầu dạng CSV từ Our World in Data.",
    )

    extract_who_csv = PythonOperator(
        task_id="extract_who_csv",
        python_callable=extract_who_csv_callable,
        doc_md="Lọc bộ WHO theo quốc gia và ngày cấu hình rồi lưu raw vào MinIO.",
    )

    extract_mysql_oltp = PythonOperator(
        task_id="extract_mysql_oltp",
        python_callable=extract_mysql_oltp_callable,
        doc_md="Xuất các bảng vaccination_centers và vaccination_records sang CSV.",
    )

    load_to_minio = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio_callable,
        doc_md="Upload các file CSV thô lên bucket `covid19-landing-zone` trong MinIO.",
    )
    transform_and_load_to_dwh = PythonOperator(
        task_id="transform_and_load_to_dwh",
        python_callable=transform_and_load_to_dwh_callable,
        doc_md=(
            "Nạp dữ liệu OWID theo quốc gia/ngày vào dim_country, dim_region, dim_date "
            "và fact_cases."
        ),
    )

    weekly_covid_kmeans = PythonOperator(
        task_id="weekly_covid_kmeans",
        python_callable=cluster_weekly_covid_callable,
        doc_md=(
            "Tổng hợp ca mắc/tử vong theo quốc gia-tuần, phân cụm K-means theo "
            "mức ca, tử vong và biến động ca, lưu vào dwh.vw_weekly_covid_clusters."
        ),
    )

    [extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh
    transform_and_load_to_dwh >> weekly_covid_kmeans
