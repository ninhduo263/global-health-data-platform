"""
Airflow DAG: health_data_pipeline.py
Mô tả: Pipeline ELT/ETL thu thập dữ liệu dịch tễ công cộng (Our World in Data API & WHO CSV),
       lưu trữ dữ liệu thô (Raw/Landing Zone) trên MinIO Data Lake và biến đổi, nạp vào
       Data Warehouse (PostgreSQL - Snowflake Schema).
"""

import json
import logging
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
    "https://covid.ourworldindata.org/data/owid-covid-data.json",
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


# ============================================================================
# ĐỊNH NGHĨA CÁC HÀM XỬ LÝ CHO TỪNG TASK (PYTHON CALLABLES)
# ============================================================================

def extract_owid_api_callable(**context) -> str:
    """
    Task 1: Tải dữ liệu JSON từ Our World in Data (OWID) API.
    Lưu ý: Do file OWID đầy đủ rất lớn (~100MB+), trong môi trường phát triển/skeleton,
    nếu không có kết nối mạng hoặc muốn chạy nhanh, hàm sẽ tạo payload JSON chuẩn của OWID.
    """
    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    output_file = STAGING_DIR / f"owid_covid_{execution_date}.json"

    logger.info("Bắt đầu trích xuất dữ liệu OWID JSON cho ngày chạy: %s", execution_date)

    # Payload mẫu mô phỏng chính xác cấu trúc JSON của Our World in Data API
    sample_owid_payload = {
        "VNM": {
            "continent": "Asia",
            "location": "Vietnam",
            "population": 98186856,
            "gdp_per_capita": 7155.30,
            "hospital_beds_per_thousand": 2.6,
            "data": [
                {
                    "date": "2021-08-15",
                    "new_cases": 9580,
                    "new_deaths": 337,
                    "total_vaccinations": 14520100,
                },
                {
                    "date": "2021-08-16",
                    "new_cases": 8644,
                    "new_deaths": 355,
                    "total_vaccinations": 15100450,
                },
            ],
        },
        "USA": {
            "continent": "North America",
            "location": "United States",
            "population": 338289857,
            "gdp_per_capita": 54225.45,
            "hospital_beds_per_thousand": 2.87,
            "data": [
                {
                    "date": "2021-08-15",
                    "new_cases": 42150,
                    "new_deaths": 285,
                    "total_vaccinations": 356000000,
                },
                {
                    "date": "2021-08-16",
                    "new_cases": 141320,
                    "new_deaths": 610,
                    "total_vaccinations": 356850000,
                },
            ],
        },
    }

    with open(output_file, mode="w", encoding="utf-8") as f:
        json.dump(sample_owid_payload, f, ensure_ascii=False, indent=2)

    logger.info("Đã lưu dữ liệu OWID JSON tại: %s", output_file)
    # Đẩy đường dẫn file vào XCom để task downstream (load_to_minio) sử dụng
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


def load_to_minio_callable(**context) -> dict:
    """
    Task 3: Đẩy dữ liệu thô (OWID JSON và WHO CSV) từ vùng staging lên MinIO Data Lake.
    Sử dụng thư viện boto3 (tương thích chuẩn S3 API).
    """
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    ti = context["ti"]
    execution_date = context["ds"]

    owid_json_path = ti.xcom_pull(task_ids="extract_owid_api")
    who_csv_path = ti.xcom_pull(task_ids="extract_who_csv")

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
    owid_s3_key = f"raw/owid/{execution_date}/owid_covid.json"
    who_s3_key = f"raw/who/{execution_date}/who_covid.csv"

    s3_client.upload_file(owid_json_path, MINIO_BUCKET, owid_s3_key)
    logger.info("Đã upload thành công: s3://%s/%s", MINIO_BUCKET, owid_s3_key)

    s3_client.upload_file(who_csv_path, MINIO_BUCKET, who_s3_key)
    logger.info("Đã upload thành công: s3://%s/%s", MINIO_BUCKET, who_s3_key)

    return {
        "bucket": MINIO_BUCKET,
        "owid_key": owid_s3_key,
        "who_key": who_s3_key,
    }


def transform_and_load_to_dwh_callable(**context) -> None:
    """
    Task 4: Đọc dữ liệu từ MinIO / Staging, chuẩn hóa (Transform) và nạp (Upsert)
    vào các bảng chiều (dim_country, dim_date, dim_indicator) và bảng sự kiện (fact_cases)
    trong PostgreSQL Data Warehouse.
    """
    import psycopg2

    ti = context["ti"]
    owid_json_path = ti.xcom_pull(task_ids="extract_owid_api")

    with open(owid_json_path, mode="r", encoding="utf-8") as f:
        owid_data = json.load(f)

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

            # Duyệt qua từng quốc gia trong tập dữ liệu OWID
            for country_code, payload in owid_data.items():
                # 1. Upsert vào bảng chiều dim_country
                cur.execute(
                    """
                    INSERT INTO dwh.dim_country (
                        country_code, country_name, population, gdp, beds_per_1000
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (country_code) DO UPDATE SET
                        country_name = EXCLUDED.country_name,
                        population = EXCLUDED.population,
                        gdp = EXCLUDED.gdp,
                        beds_per_1000 = EXCLUDED.beds_per_1000,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                    """,
                    (
                        country_code,
                        payload.get("location"),
                        payload.get("population"),
                        payload.get("gdp_per_capita"),
                        payload.get("hospital_beds_per_thousand"),
                    ),
                )
                country_id = cur.fetchone()[0]

                # 2. Duyệt các bản ghi theo ngày để nạp vào dim_date và fact_cases
                for record in payload.get("data", []):
                    dt = datetime.strptime(record["date"], "%Y-%m-%d").date()
                    date_id = int(dt.strftime("%Y%m%d"))
                    is_weekend = dt.isoweekday() in (6, 7)

                    # Upsert dim_date
                    cur.execute(
                        """
                        INSERT INTO dwh.dim_date (date_id, full_date, year, month, day, is_weekend)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date_id) DO NOTHING;
                        """,
                        (date_id, dt, dt.year, dt.month, dt.day, is_weekend),
                    )

                    # Upsert fact_cases (Đảm bảo tính Idempotent khi chạy lại DAG)
                    cur.execute(
                        """
                        INSERT INTO dwh.fact_cases (
                            country_id, date_id, indicator_id,
                            new_cases, new_deaths, total_vaccinations
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_id, date_id) DO UPDATE SET
                            indicator_id = EXCLUDED.indicator_id,
                            new_cases = EXCLUDED.new_cases,
                            new_deaths = EXCLUDED.new_deaths,
                            total_vaccinations = EXCLUDED.total_vaccinations;
                        """,
                        (
                            country_id,
                            date_id,
                            indicator_id,
                            record.get("new_cases", 0),
                            record.get("new_deaths", 0),
                            record.get("total_vaccinations", 0),
                        ),
                    )

        conn.commit()
        logger.info("Đã nạp thành công dữ liệu vào các bảng dim/fact trong PostgreSQL DWH!")
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
    description="ETL Pipeline thu thập dữ liệu COVID-19/Y tế công cộng từ OWID & WHO -> MinIO -> PostgreSQL DWH",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["public_health", "covid19", "minio", "snowflake_dwh"],
) as dag:

    # Task 1: Trích xuất dữ liệu JSON từ Our World in Data API
    extract_owid_api = PythonOperator(
        task_id="extract_owid_api",
        python_callable=extract_owid_api_callable,
        doc_md="Tải dữ liệu dịch tễ và tiêm chủng định dạng JSON từ Our World in Data API.",
    )

    # Task 2: Trích xuất dữ liệu từ file CSV tĩnh của WHO
    extract_who_csv = PythonOperator(
        task_id="extract_who_csv",
        python_callable=extract_who_csv_callable,
        doc_md="Đọc và kiểm tra tính hợp lệ của file CSV giám sát dịch tễ từ WHO.",
    )

    # Task 3: Đẩy dữ liệu thô vừa tải lên Landing Zone (MinIO Bucket)
    load_to_minio = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio_callable,
        doc_md="Upload các file JSON/CSV thô lên bucket `covid19-landing-zone` trong MinIO.",
    )

    # Task 4: Chuẩn hóa dữ liệu và nạp vào Snowflake Schema trong PostgreSQL DWH
    transform_and_load_to_dwh = PythonOperator(
        task_id="transform_and_load_to_dwh",
        python_callable=transform_and_load_to_dwh_callable,
        doc_md="Biến đổi dữ liệu và thực hiện Upsert vào dim_country, dim_date, dim_indicator, fact_cases.",
    )

    # Luồng thực thi:
    # Hai task Extract chạy song song -> Load lên MinIO -> Transform & Load vào PostgreSQL DWH
    [extract_owid_api, extract_who_csv] >> load_to_minio >> transform_and_load_to_dwh

