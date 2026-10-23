"""
Airflow DAG: health_data_pipeline.py
Mô tả: Pipeline ELT/ETL thu thập dữ liệu dịch tễ công cộng (Our World in Data API & WHO CSV),
       lưu trữ dữ liệu thô (Raw/Landing Zone) trên MinIO Data Lake và biến đổi, nạp vào
       Data Warehouse (PostgreSQL - Snowflake Schema).
"""

import logging
import math
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Thiết lập Logger chuẩn của Airflow
logger = logging.getLogger("airflow.task")

# ============================================================================
# CẤU HÌNH KẾT NỐI & ĐƯỜNG DẪN MẶC ĐỊNH (Đọc từ biến môi trường của Container)
# ============================================================================
OWID_API_URL = os.getenv(
    "OWID_API_URL",
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
)
WHO_CSV_SOURCE_PATH = os.getenv(
    "WHO_CSV_SOURCE_PATH",
    "/opt/airflow/data/raw/who_covid_sample.csv",
)
STAGING_DIR = Path("/tmp/health_pipeline_staging")

# Cấu hình MinIO (S3-Compatible Object Storage)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "covid19-landing-zone")

# Cấu hình PostgreSQL Data Warehouse
DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB = os.getenv("DWH_DB", "health_dwh")
DWH_USER = os.getenv("DWH_USER", "dwh_admin")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_password")

# MySQL OLTP source
OLTP_HOST = os.getenv("OLTP_HOST", "mysql-oltp")
OLTP_PORT = int(os.getenv("OLTP_PORT", "3306"))
OLTP_DATABASE = os.getenv("OLTP_DATABASE", "vaccination_oltp")
OLTP_USER = os.getenv("OLTP_USER", "root")
OLTP_PASSWORD = os.getenv("OLTP_PASSWORD", "root_password")


# ============================================================================
# ĐỊNH NGHĨA CÁC HÀM XỬ LÝ CHO TỪNG TASK (PYTHON CALLABLES)
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
    """
    Task 2: Đọc/sao chép file CSV tĩnh của WHO từ thư mục mount sang vùng staging.
    """
    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    target_csv = STAGING_DIR / f"who_covid_{execution_date}.csv"

    logger.info("Kiểm tra file CSV tĩnh của WHO tại: %s", WHO_CSV_SOURCE_PATH)

    if os.path.exists(WHO_CSV_SOURCE_PATH):
        shutil.copy2(WHO_CSV_SOURCE_PATH, target_csv)
        logger.info("Đã sao chép file CSV WHO sang staging: %s", target_csv)
    else:
        logger.warning("Không tìm thấy file tại %s, tạo file CSV mẫu...", WHO_CSV_SOURCE_PATH)
        sample_csv_content = (
            "date_reported,country_code,country_name,who_region,new_cases,new_deaths\n"
            "2021-08-15,VNM,Vietnam,WPRO,9580,337\n"
            "2021-08-16,VNM,Vietnam,WPRO,8644,355\n"
        )
        target_csv.write_text(sample_csv_content, encoding="utf-8")

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
        database=OLTP_DATABASE,
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
                    "record_id, center_id, vaccine_type, dose_number, "
                    "vaccination_date, created_at, updated_at"
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
    """
    Task 3: Đẩy dữ liệu OWID/WHO và OLTP thô từ staging lên MinIO Data Lake.
    Sử dụng thư viện boto3 (tương thích chuẩn S3 API).
    """
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    ti = context["ti"]
    execution_date = context["ds"]

    owid_csv_path = ti.xcom_pull(task_ids="extract_owid_api")
    who_csv_path = ti.xcom_pull(task_ids="extract_who_csv")
    mysql_oltp_paths = ti.xcom_pull(task_ids="extract_mysql_oltp")

    logger.info("Kết nối tới MinIO Endpoint: %s", MINIO_ENDPOINT)
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Tự động kiểm tra và khởi tạo Bucket nếu chưa tồn tại (Idempotent)
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
        logger.info("Bucket '%s' đã tồn tại.", MINIO_BUCKET)
    except ClientError:
        logger.info("Khởi tạo mới Bucket '%s' trên MinIO...", MINIO_BUCKET)
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
    Task 4: Đọc dữ liệu từ MinIO / Staging, chuẩn hóa (Transform) và nạp (Upsert)
    vào các bảng chiều (dim_country, dim_date, dim_indicator) và bảng sự kiện (fact_cases)
    trong PostgreSQL Data Warehouse.
    """
    import csv

    import psycopg2
    from psycopg2.extras import execute_values

    ti = context["ti"]
    owid_csv_path = ti.xcom_pull(task_ids="extract_owid_api")
    if not owid_csv_path or not os.path.isfile(owid_csv_path):
        raise FileNotFoundError(f"OWID CSV snapshot was not found: {owid_csv_path}")

    continent_codes = {
        "Africa": "AF",
        "Asia": "AS",
        "Europe": "EU",
        "North America": "NA",
        "Oceania": "OC",
        "South America": "SA",
    }

    def integer_value(value: str) -> int:
            """Convert missing, negative, or malformed OWID counts to zero."""
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError):
                return 0
            if not math.isfinite(number) or number < 0:
                return 0
            return int(number)

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

    logger.info("Kết nối tới PostgreSQL DWH tại %s:%s/%s", DWH_HOST, DWH_PORT, DWH_DB)
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

            # Lấy ID của chỉ số mặc định trong bảng dim_indicator
            cur.execute(
                """
                INSERT INTO dwh.dim_indicator (indicator_code, description)
                VALUES ('COVID19_DAILY_SURVEILLANCE', 'Giám sát dịch tễ COVID-19 hàng ngày')
                ON CONFLICT (indicator_code) DO UPDATE
                    SET description = EXCLUDED.description
                RETURNING id;
                """
            )
            indicator_id = cur.fetchone()[0]

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
                        country_id, date_id, indicator_id, new_cases, new_deaths,
                        total_vaccinations, new_vaccinations, people_vaccinated,
                        people_fully_vaccinated, total_cases, total_deaths
                    ) VALUES %s
                    ON CONFLICT (country_id, date_id) DO UPDATE SET
                        indicator_id = EXCLUDED.indicator_id,
                        new_cases = EXCLUDED.new_cases,
                        new_deaths = EXCLUDED.new_deaths,
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
    except Exception as exc:
        conn.rollback()
        logger.error("Lỗi trong quá trình nạp dữ liệu vào DWH: %s", exc)
        raise
    finally:
        conn.close()


# ============================================================================
# KHỞI TẠO ĐỐI TƯỢNG DAG & THIẾT LẬP LUỒNG PHỤ THUỘC (TASK DEPENDENCIES)
# ============================================================================

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="health_data_pipeline",
    default_args=default_args,
    description=(
        "Nạp lịch sử COVID-19 toàn cầu theo quốc gia/ngày từ OWID cùng dữ liệu WHO "
        "vào MinIO và PostgreSQL DWH."
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["public_health", "covid19", "minio", "snowflake_dwh"],
) as dag:

    # Task 1: Tải snapshot CSV COVID-19 toàn cầu từ Our World in Data
    extract_owid_api = PythonOperator(
        task_id="extract_owid_api",
        python_callable=extract_owid_api_callable,
        doc_md="Tải dữ liệu COVID-19 lịch sử toàn cầu dạng CSV từ Our World in Data.",
    )

    # Task 2: Trích xuất dữ liệu từ file CSV tĩnh của WHO
    extract_who_csv = PythonOperator(
        task_id="extract_who_csv",
        python_callable=extract_who_csv_callable,
        doc_md="Đọc và kiểm tra tính hợp lệ của file CSV giám sát dịch tễ từ WHO.",
    )

    # Task 3: Trích xuất dữ liệu giao dịch và trung tâm tiêm chủng từ MySQL
    extract_mysql_oltp = PythonOperator(
        task_id="extract_mysql_oltp",
        python_callable=extract_mysql_oltp_callable,
        doc_md="Xuất các bảng vaccination_centers và vaccination_records sang CSV.",
    )

    # Task 4: Đẩy dữ liệu thô vừa tải lên Landing Zone (MinIO Bucket)
    load_to_minio = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio_callable,
        doc_md="Upload các file CSV thô lên bucket `covid19-landing-zone` trong MinIO.",
    )

    # Task 5: Chuẩn hóa dữ liệu và nạp vào Snowflake Schema trong PostgreSQL DWH
    transform_and_load_to_dwh = PythonOperator(
        task_id="transform_and_load_to_dwh",
        python_callable=transform_and_load_to_dwh_callable,
        doc_md=(
            "Nạp dữ liệu OWID theo quốc gia/ngày vào dim_country, dim_region, dim_date "
            "và fact_cases."
        ),
    )

    # Luồng thực thi:
    # Hai task Extract chạy song song -> Load lên MinIO -> Transform & Load vào PostgreSQL DWH
    [extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh
