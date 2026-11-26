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
            for table_name in ("vaccination_centers", "patients", "vaccination_records"):
                cursor.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (OLTP_DB, table_name),
                )
                fieldnames = [
                    row["COLUMN_NAME"]
                    for row in cursor.fetchall()
                    if not (table_name == "patients" and row["COLUMN_NAME"] == "full_name")
                ]
                if not fieldnames:
                    raise ValueError(
                        f"MySQL table {OLTP_DB}.{table_name} was not found or has no columns"
                    )

                quoted_columns = ", ".join(f"`{column}`" for column in fieldnames)
                cursor.execute(f"SELECT {quoted_columns} FROM `{table_name}`")
                rows = cursor.fetchall()
                output_file = STAGING_DIR / f"{table_name}_{execution_date}.csv"
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
                DROP VIEW IF EXISTS dwh.vw_dashboard_cases;

                ALTER TABLE dwh.fact_cases
                    ADD COLUMN IF NOT EXISTS new_tests BIGINT NOT NULL DEFAULT 0
                        CHECK (new_tests >= 0),
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
                    else:
                        cur.execute(
                            """
                            UPDATE dwh.dim_country
                            SET population = COALESCE(%s, population),
                                gdp = COALESCE(%s, gdp),
                                beds_per_1000 = COALESCE(%s, beds_per_1000),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            """,
                            (
                                nullable_integer(record.get("population")),
                                nullable_float(record.get("gdp_per_capita")),
                                nullable_float(record.get("hospital_beds_per_thousand")),
                                country_ids[country_code],
                            ),
                        )

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
    """Cluster weekly country-level COVID-19 observations, evaluate K, and persist profiles + PCA to DWH."""
    import psycopg2
    from psycopg2.extras import execute_values

    from covid_cluster_transitions import run_cluster_transition_mining
    from weekly_covid_kmeans import run_weekly_clustering

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

            raw_requested = os.getenv("COVID_KMEANS_CLUSTERS", "4").strip()
            requested_clusters = (
                "auto" if raw_requested.lower() == "auto" else int(raw_requested)
            )
            k_range = os.getenv("COVID_KMEANS_K_RANGE", "2-8")

            clustering_result = run_weekly_clustering(
                weekly_records,
                requested_clusters=requested_clusters,
                k_range=k_range,
            )
            clustered_records = clustering_result["records"]
            evaluations = clustering_result["evaluations"]
            profiles = clustering_result["profiles"]
            cluster_count = clustering_result["cluster_count"]
            model_version = "weekly-kmeans-v1"

            cur.execute(
                "DELETE FROM dwh.fact_weekly_covid_cluster WHERE model_version = %s",
                (model_version,),
            )
            cur.execute(
                "DELETE FROM dwh.fact_kmeans_evaluation WHERE model_version = %s",
                (model_version,),
            )
            cur.execute(
                "DELETE FROM dwh.fact_kmeans_cluster_profile WHERE model_version = %s",
                (model_version,),
            )
            cur.execute(
                "DELETE FROM dwh.fact_cluster_transition WHERE model_version = %s",
                (model_version,),
            )
            cur.execute(
                "DELETE FROM dwh.fact_cluster_dwell_time WHERE model_version = %s",
                (model_version,),
            )

            cluster_values = [
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
                    model_version,
                    record["cluster_label"],
                    record["pca_1"],
                    record["pca_2"],
                )
                for record in clustered_records
            ]
            execute_values(
                cur,
                """
                INSERT INTO dwh.fact_weekly_covid_cluster (
                    week_start, country_code, country_name, cluster_id, cluster_count,
                    weekly_cases, weekly_deaths, population, cases_per_100k,
                    deaths_per_100k, case_growth_pct, model_version,
                    cluster_label, pca_1, pca_2
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
                    cluster_label = EXCLUDED.cluster_label,
                    pca_1 = EXCLUDED.pca_1,
                    pca_2 = EXCLUDED.pca_2,
                    computed_at = CURRENT_TIMESTAMP
                """,
                cluster_values,
                page_size=1000,
            )

            if evaluations:
                eval_values = [
                    (
                        model_version,
                        ev["k"],
                        ev["inertia"],
                        ev["silhouette"],
                        ev["davies_bouldin"],
                        ev["calinski_harabasz"],
                        ev["total_rows"],
                        ev["silhouette_sample_rows"],
                        ev["is_selected"],
                        ev["is_best_silhouette"],
                    )
                    for ev in evaluations
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_kmeans_evaluation (
                        model_version, k, inertia, silhouette, davies_bouldin,
                        calinski_harabasz, total_rows, silhouette_sample_rows,
                        is_selected, is_best_silhouette
                    ) VALUES %s
                    """,
                    eval_values,
                )

            if profiles:
                profile_values = [
                    (
                        model_version,
                        prof["cluster_id"],
                        prof["cluster_label"],
                        prof["member_count"],
                        prof["member_share"],
                        prof["avg_cases_per_100k"],
                        prof["median_cases_per_100k"],
                        prof["avg_deaths_per_100k"],
                        prof["median_case_growth_pct"],
                        prof["centroid_log_cases"],
                        prof["centroid_log_deaths"],
                        prof["centroid_log_change"],
                        prof["avg_silhouette"],
                    )
                    for prof in profiles
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_kmeans_cluster_profile (
                        model_version, cluster_id, cluster_label, member_count,
                        member_share, avg_cases_per_100k, median_cases_per_100k,
                        avg_deaths_per_100k, median_case_growth_pct,
                        centroid_log_cases, centroid_log_deaths, centroid_log_change,
                        avg_silhouette
                    ) VALUES %s
                    """,
                    profile_values,
                )

            transition_result = run_cluster_transition_mining(clustered_records)
            transitions = transition_result["transitions"]
            dwell_times = transition_result["dwell_times"]

            if transitions:
                trans_values = [
                    (
                        model_version,
                        t["from_cluster_id"],
                        t["from_cluster_label"],
                        t["to_cluster_id"],
                        t["to_cluster_label"],
                        t["transition_count"],
                        t["from_total_count"],
                        t["transition_probability"],
                        t["is_self_transition"],
                        t["transition_type_vi"],
                    )
                    for t in transitions
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_cluster_transition (
                        model_version, from_cluster_id, from_cluster_label,
                        to_cluster_id, to_cluster_label, transition_count,
                        from_total_count, transition_probability,
                        is_self_transition, transition_type_vi
                    ) VALUES %s
                    """,
                    trans_values,
                )

            if dwell_times:
                dwell_values = [
                    (
                        model_version,
                        d["cluster_id"],
                        d["cluster_label"],
                        d["episode_count"],
                        d["total_weeks"],
                        d["avg_dwell_weeks"],
                        d["median_dwell_weeks"],
                        d["max_dwell_weeks"],
                        d["retention_probability"],
                    )
                    for d in dwell_times
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_cluster_dwell_time (
                        model_version, cluster_id, cluster_label, episode_count,
                        total_weeks, avg_dwell_weeks, median_dwell_weeks,
                        max_dwell_weeks, retention_probability
                    ) VALUES %s
                    """,
                    dwell_values,
                )

        conn.commit()
        logger.info(
            "Persisted %s weekly country observations into %s K-means clusters (evaluated %s K candidates)",
            len(clustered_records),
            cluster_count,
            len(evaluations),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def early_warning_callable(**context) -> None:
    """Train and evaluate XGBoost early warning surge classifier and persist alerts to DWH."""
    import psycopg2
    from psycopg2.extras import execute_values

    from covid_early_warning import run_early_warning_pipeline

    horizon_weeks = int(os.getenv("COVID_EW_HORIZON_WEEKS", "2"))
    surge_ratio = float(os.getenv("COVID_EW_SURGE_RATIO", "1.5"))
    min_cases_per_100k = float(os.getenv("COVID_EW_MIN_CASES_PER_100K", "10"))
    test_fraction = float(os.getenv("COVID_EW_TEST_FRACTION", "0.2"))
    include_cluster = os.getenv("COVID_EW_INCLUDE_CLUSTER", "true").strip().lower() not in (
        "0",
        "false",
        "no",
    )
    model_version = os.getenv("COVID_EW_MODEL_VERSION", "early-warning-v1")

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
                WITH weekly_vax AS (
                    SELECT
                        c.country_code,
                        date_trunc('week', d.full_date)::date AS week_start,
                        MAX(f.people_vaccinated)::bigint AS people_vaccinated,
                        MAX(f.people_fully_vaccinated)::bigint AS people_fully_vaccinated,
                        COUNT(DISTINCT d.full_date)::int AS observed_days
                    FROM dwh.fact_cases AS f
                    JOIN dwh.dim_country AS c ON c.id = f.country_id
                    JOIN dwh.dim_date AS d ON d.date_id = f.date_id
                    WHERE c.population > 0
                    GROUP BY c.country_code, date_trunc('week', d.full_date)::date
                )
                SELECT
                    kc.week_start,
                    kc.country_code,
                    kc.country_name,
                    kc.population,
                    kc.weekly_cases,
                    kc.weekly_deaths,
                    kc.cases_per_100k::double precision,
                    kc.deaths_per_100k::double precision,
                    kc.case_growth_pct::double precision,
                    kc.cluster_id,
                    COALESCE(wv.people_vaccinated, 0) AS people_vaccinated,
                    COALESCE(wv.people_fully_vaccinated, 0) AS people_fully_vaccinated,
                    COALESCE(wv.observed_days, 7) AS observed_days
                FROM dwh.fact_weekly_covid_cluster AS kc
                LEFT JOIN weekly_vax AS wv
                    ON wv.country_code = kc.country_code
                   AND wv.week_start = kc.week_start
                WHERE kc.model_version = 'weekly-kmeans-v1'
                ORDER BY kc.week_start, kc.country_code
                """
            )
            columns = (
                "week_start",
                "country_code",
                "country_name",
                "population",
                "weekly_cases",
                "weekly_deaths",
                "cases_per_100k",
                "deaths_per_100k",
                "case_growth_pct",
                "cluster_id",
                "people_vaccinated",
                "people_fully_vaccinated",
                "observed_days",
            )
            records = [dict(zip(columns, row)) for row in cur.fetchall()]
            if not records:
                raise ValueError(
                    "No weekly clustered records found in dwh.fact_weekly_covid_cluster; run weekly_covid_kmeans first"
                )

            ew_result = run_early_warning_pipeline(
                records,
                horizon_weeks=horizon_weeks,
                surge_ratio=surge_ratio,
                min_cases_per_100k=min_cases_per_100k,
                test_fraction=test_fraction,
                include_cluster=include_cluster,
            )

            cur.execute(
                "DELETE FROM dwh.fact_early_warning WHERE model_version = %s",
                (model_version,),
            )
            cur.execute(
                "DELETE FROM dwh.fact_early_warning_evaluation WHERE model_version = %s",
                (model_version,),
            )
            cur.execute(
                "DELETE FROM dwh.fact_early_warning_feature_importance WHERE model_version = %s",
                (model_version,),
            )

            pred_values = [
                (
                    p["week_start"],
                    p["country_code"],
                    p["country_name"],
                    model_version,
                    p["model_name"],
                    p["horizon_weeks"],
                    p["prediction_split"],
                    p["cases_per_100k"],
                    p["case_growth_pct"],
                    p["cluster_id"],
                    p["surge_probability"],
                    p["decision_threshold"],
                    p["predicted_surge"],
                    p["alert_level"],
                    p["actual_surge"],
                    p["future_cases_per_100k"],
                    p.get("top_reasons_vi"),
                )
                for p in ew_result["predictions"]
            ]
            if pred_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_early_warning (
                        week_start, country_code, country_name, model_version, model_name,
                        horizon_weeks, prediction_split, cases_per_100k, case_growth_pct,
                        cluster_id, surge_probability, decision_threshold, predicted_surge,
                        alert_level, actual_surge, future_cases_per_100k, top_reasons_vi
                    ) VALUES %s
                    """,
                    pred_values,
                    page_size=1000,
                )

            eval_values = [
                (
                    model_version,
                    ev["model_name"],
                    ev["is_primary"],
                    ev["decision_threshold"],
                    ev["accuracy"],
                    ev["precision_score"],
                    ev["recall_score"],
                    ev["f1_score"],
                    ev["roc_auc"],
                    ev["pr_auc"],
                    ev["true_positive"],
                    ev["false_positive"],
                    ev["true_negative"],
                    ev["false_negative"],
                    ev["train_rows"],
                    ev["test_rows"],
                    ev["test_positive_rate"],
                    ev["test_start_week"],
                    ev["test_end_week"],
                    ev["horizon_weeks"],
                    ev["surge_ratio"],
                    ev["min_cases_per_100k"],
                )
                for ev in ew_result["evaluations"]
            ]
            if eval_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_early_warning_evaluation (
                        model_version, model_name, is_primary, decision_threshold,
                        accuracy, precision_score, recall_score, f1_score, roc_auc, pr_auc,
                        true_positive, false_positive, true_negative, false_negative,
                        train_rows, test_rows, test_positive_rate, test_start_week,
                        test_end_week, horizon_weeks, surge_ratio, min_cases_per_100k
                    ) VALUES %s
                    """,
                    eval_values,
                )

            imp_values = [
                (
                    model_version,
                    imp["model_name"],
                    imp["feature_name"],
                    imp["importance"],
                    imp["importance_rank"],
                )
                for imp in ew_result["feature_importances"]
            ]
            if imp_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_early_warning_feature_importance (
                        model_version, model_name, feature_name, importance, importance_rank
                    ) VALUES %s
                    """,
                    imp_values,
                )

        conn.commit()

        # Lưu artifact mô hình XGBoost đã huấn luyện phục vụ mô phỏng What-If trên AI Portal
        try:
            import pickle

            model_dir = Path("/opt/airflow/data/models")
            model_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = model_dir / "early_warning_model.pkl"
            with artifact_path.open("wb") as f:
                pickle.dump(
                    {
                        "model_name": ew_result["primary_model_name"],
                        "estimator": ew_result["primary_estimator"],
                        "decision_threshold": ew_result["decision_threshold"],
                        "feature_cols": ew_result["feature_cols"],
                        "horizon_weeks": horizon_weeks,
                        "model_version": model_version,
                    },
                    f,
                )
            logger.info("Saved trained early-warning model artifact to %s", artifact_path)
        except Exception as exc:
            logger.warning("Could not persist early-warning model artifact: %s", exc)

        logger.info(
            "Persisted %s early-warning rows (primary=%s, cutoff=%s) for model_version=%s",
            len(pred_values),
            ew_result["primary_model_name"],
            ew_result["cutoff_week"],
            model_version,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mine_patient_rules_callable(**context) -> None:
    """Mine association rules on anonymized CDC patients + vaccination records and persist to DWH."""
    import psycopg2
    from psycopg2.extras import execute_values

    from patient_association_rules import mine_patient_association_rules

    min_support = float(os.getenv("COVID_AR_MIN_SUPPORT", "0.015"))
    min_confidence = float(os.getenv("COVID_AR_MIN_CONFIDENCE", "0.20"))
    min_lift = float(os.getenv("COVID_AR_MIN_LIFT", "1.08"))
    top_k = int(os.getenv("COVID_AR_TOP_K", "40"))
    model_version = os.getenv("COVID_AR_MODEL_VERSION", "apriori-v1")

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
                WITH vax_by_patient AS (
                    SELECT
                        f.patient_id,
                        COUNT(*)::int AS dose_count,
                        (ARRAY_AGG(f.vaccine_type ORDER BY f.dose_number ASC))[1] AS primary_vaccine
                    FROM dwh.fact_vaccination AS f
                    WHERE f.patient_id IS NOT NULL
                    GROUP BY f.patient_id
                )
                SELECT
                    p.patient_id,
                    p.birth_year,
                    COALESCE(d.year, 2022) AS reported_year,
                    p.gender,
                    p.province,
                    p.health_status,
                    p.comorbidity,
                    COALESCE(v.dose_count, 0) AS dose_count,
                    v.primary_vaccine
                FROM dwh.dim_patient AS p
                LEFT JOIN dwh.dim_date AS d ON d.date_id = p.reported_date_id
                LEFT JOIN vax_by_patient AS v ON v.patient_id = p.patient_id
                WHERE NOT p.is_placeholder
                ORDER BY p.patient_id
                """
            )
            cols = (
                "patient_id",
                "birth_year",
                "reported_year",
                "gender",
                "province",
                "health_status",
                "comorbidity",
                "dose_count",
                "primary_vaccine",
            )
            patient_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            if not patient_rows:
                logger.warning("No anonymized patient records found in dwh.dim_patient; skipping association rules")
                return

            rules = mine_patient_association_rules(
                patient_rows,
                min_support=min_support,
                min_confidence=min_confidence,
                min_lift=min_lift,
                top_k=top_k,
            )

            cur.execute(
                "DELETE FROM dwh.fact_patient_association_rules WHERE model_version = %s",
                (model_version,),
            )
            if rules:
                rule_values = [
                    (
                        model_version,
                        r["rule_id"],
                        r["antecedents"],
                        r["consequent"],
                        r["rule_text_vi"],
                        r["antecedent_count"],
                        r["support_count"],
                        r["total_transactions"],
                        r["support"],
                        r["confidence"],
                        r["lift"],
                        r["leverage"],
                        r["conviction"],
                        r["target_category_vi"],
                    )
                    for r in rules
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_patient_association_rules (
                        model_version, rule_id, antecedents, consequent, rule_text_vi,
                        antecedent_count, support_count, total_transactions,
                        support, confidence, lift, leverage, conviction, target_category_vi
                    ) VALUES %s
                    """,
                    rule_values,
                )
        conn.commit()
        logger.info(
            "Persisted %s association rules mined from %s anonymized CDC patients",
            len(rules),
            len(patient_rows),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def anomaly_detection_callable(**context) -> None:
    """Detect epidemiological weekly anomalies via Isolation Forest and persist to DWH."""
    import psycopg2
    from psycopg2.extras import execute_values

    from covid_anomaly_detection import detect_weekly_anomalies

    contamination = float(os.getenv("COVID_ANOMALY_CONTAMINATION", "0.02"))
    model_version = os.getenv("COVID_ANOMALY_MODEL_VERSION", "iforest-v1")

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
                SELECT
                    week_start,
                    country_code,
                    country_name,
                    cases_per_100k::double precision,
                    deaths_per_100k::double precision,
                    case_growth_pct::double precision
                FROM dwh.fact_weekly_covid_cluster
                WHERE model_version = 'weekly-kmeans-v1'
                ORDER BY week_start, country_code
                """
            )
            cols = (
                "week_start",
                "country_code",
                "country_name",
                "cases_per_100k",
                "deaths_per_100k",
                "case_growth_pct",
            )
            weekly_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            if not weekly_rows:
                raise ValueError("No weekly cluster rows found for Isolation Forest anomaly detection")

            anomalies = detect_weekly_anomalies(
                weekly_rows,
                contamination=contamination,
            )

            cur.execute(
                "DELETE FROM dwh.fact_covid_anomalies WHERE model_version = %s",
                (model_version,),
            )
            if anomalies:
                anom_values = [
                    (
                        model_version,
                        a["week_start"],
                        a["country_code"],
                        a["country_name"],
                        a["cases_per_100k"],
                        a["deaths_per_100k"],
                        a["case_growth_pct"],
                        a["case_z_score"],
                        a["death_z_score"],
                        a["anomaly_score"],
                        a["anomaly_type_vi"],
                        a["explanation_vi"],
                    )
                    for a in anomalies
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_covid_anomalies (
                        model_version, week_start, country_code, country_name,
                        cases_per_100k, deaths_per_100k, case_growth_pct,
                        case_z_score, death_z_score, anomaly_score,
                        anomaly_type_vi, explanation_vi
                    ) VALUES %s
                    """,
                    anom_values,
                    page_size=1000,
                )
        conn.commit()
        logger.info("Persisted %s Isolation Forest anomalies into dwh.fact_covid_anomalies", len(anomalies))
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
    tags=["public_health", "who", "owid", "kmeans", "xgboost", "early_warning", "snowflake_dwh"],
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
        doc_md="Xuất các bảng vaccination_centers, patients (đã bỏ full_name) và vaccination_records sang CSV.",
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
            "Tổng hợp ca mắc/tử vong theo quốc gia-tuần, đánh giá số cụm K (Elbow/Silhouette), "
            "tạo hồ sơ cụm và tọa độ PCA 2D vào DWH."
        ),
    )

    covid_early_warning = PythonOperator(
        task_id="covid_early_warning",
        python_callable=early_warning_callable,
        doc_md=(
            "Huấn luyện và đánh giá mô hình cảnh báo sớm bùng phát dịch (XGBoost vs Logistic Regression "
            "vs Baseline luật) theo chuỗi thời gian có purge gap, xuất cảnh báo và độ quan trọng đặc trưng."
        ),
    )

    patient_association_rules = PythonOperator(
        task_id="patient_association_rules",
        python_callable=mine_patient_rules_callable,
        doc_md=(
            "Khai phá luật kết hợp (Apriori / FP-Growth) giữa nhóm tuổi, giới tính, bệnh nền, "
            "số mũi tiêm và tình trạng nhiễm COVID-19 trên dữ liệu bệnh nhân ẩn danh CDC."
        ),
    )

    covid_anomaly_detection = PythonOperator(
        task_id="covid_anomaly_detection",
        python_callable=anomaly_detection_callable,
        doc_md=(
            "Phát hiện điểm bất thường dịch tễ và nhiễu báo cáo bằng Isolation Forest "
            "trên 56.477 quan sát tuần–quốc gia."
        ),
    )

    [extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh
    transform_and_load_to_dwh >> weekly_covid_kmeans >> [
        covid_early_warning,
        patient_association_rules,
        covid_anomaly_detection,
    ]
