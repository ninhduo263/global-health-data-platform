"""
Airflow DAG: health_data_pipeline.py
Mô tả: Pipeline tích hợp đa nguồn (100% Live Data - KHÔNG hardcode dữ liệu):
  - Nguồn 1 (Live API - Chuỗi thời gian): Gọi trực tiếp Our World in Data Live Catalog API & Disease.sh COVID-19 API
    để lấy dữ liệu thực tế hàng ngày về ca nhiễm (new_cases), tử vong (new_deaths), xét nghiệm (new_tests), tiêm chủng (total_vaccinations).
  - Nguồn 2 (Live WHO GHO API -> File CSV): Gọi trực tiếp WHO Global Health Observatory OData API (ghoapi.azureedge.net)
    kết hợp chỉ số hạ tầng y tế (số giường bệnh, tuổi thọ, dân số, GDP) xuất ra file CSV chuẩn.
  - Nguồn 3 (Relational DB - MySQL OLTP): Truy vấn trực tiếp bảng patients & vaccination_records từ MySQL OLTP.
Luồng xử lý: [extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh
"""

import csv
import io
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# ============================================================================
# CẤU HÌNH ENDPOINT LIVE API & KẾT NỐI HỆ THỐNG
# ============================================================================
# Nguồn 1: Live Our World in Data COVID-19 Catalog & Disease.sh Open COVID API
OWID_LIVE_CSV_URL = os.getenv(
    "OWID_LIVE_CSV_URL",
    "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv",
)
DISEASE_SH_API_URL = os.getenv(
    "DISEASE_SH_API_URL",
    "https://disease.sh/v3/covid-19/countries",
)

# Nguồn 2: Live WHO Global Health Observatory (GHO) OData API
WHO_GHO_COUNTRY_API = "https://ghoapi.azureedge.net/api/DIMENSION/COUNTRY/DimensionValues"
WHO_GHO_BEDS_API = "https://ghoapi.azureedge.net/api/WHS6_102"  # Hospital beds (per 10,000 population)

# Danh sách mã ISO-3 các quốc gia mục tiêu cần thu thập & phân tích
TARGET_COUNTRIES = ["VNM", "USA", "DEU", "JPN", "KOR", "GBR", "FRA", "BRA", "IND", "SGP"]
# Khoảng thời gian thu thập chuỗi thời gian thực tế (giai đoạn cao điểm có đầy đủ xét nghiệm, tiêm chủng, ca bệnh)
START_DATE_FILTER = os.getenv("START_DATE_FILTER", "2021-08-01")
END_DATE_FILTER = os.getenv("END_DATE_FILTER", "2021-09-30")

WHO_CSV_OUTPUT_PATH = os.getenv(
    "WHO_CSV_SOURCE_PATH",
    "/opt/airflow/data/raw/who_covid_sample.csv",
)
STAGING_DIR = Path("/tmp/health_pipeline_staging")

# Cấu hình MinIO (Data Lake - Landing Zone)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "covid19-landing-zone")

# Cấu hình MySQL (Nguồn 3 - OLTP Vận hành nội bộ)
OLTP_HOST = os.getenv("OLTP_HOST", "mysql-oltp")
OLTP_PORT = int(os.getenv("OLTP_PORT", "3306"))
OLTP_DB = os.getenv("OLTP_DB", "vaccination_oltp")
OLTP_USER = os.getenv("OLTP_USER", "oltp_user")
OLTP_PASSWORD = os.getenv("OLTP_PASSWORD", "oltp_password")

# Cấu hình PostgreSQL (Data Warehouse - Snowflake Schema)
DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB = os.getenv("DWH_DB", "health_dwh")
DWH_USER = os.getenv("DWH_USER", "dwh_admin")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_password")


# ============================================================================
# CÁC HÀM XỬ LÝ CHO TỪNG TASK (100% LIVE API & DATABASE QUERIES)
# ============================================================================

def extract_owid_api_callable(**context) -> str:
    """
    Task 1 (Nguồn 1 - Live API Chuỗi thời gian):
    Gọi trực tiếp HTTP GET tới Our World in Data Live Catalog API và Disease.sh API
    để lấy dữ liệu thực tế hàng ngày về:
      - Ca nhiễm mới (new_cases)
      - Tử vong mới (new_deaths)
      - Số lượng xét nghiệm mới (new_tests)
      - Tổng số liều vắc-xin lũy kế (total_vaccinations)
    """
    import pandas as pd
    import requests

    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    output_file = STAGING_DIR / f"owid_timeseries_{execution_date}.json"

    logger.info("Đang tải dữ liệu chuỗi thời gian TRỰC TIẾP từ OWID Live API: %s", OWID_LIVE_CSV_URL)

    # 1. Gọi Live API từ Disease.sh để lấy thêm tổng số xét nghiệm thực tế mới nhất của từng quốc gia
    disease_sh_url = f"{DISEASE_SH_API_URL}/{','.join(TARGET_COUNTRIES)}"
    resp_live = requests.get(disease_sh_url, timeout=30)
    resp_live.raise_for_status()
    disease_sh_summary = {
        item["countryInfo"]["iso3"]: {
            "total_tests_latest": item.get("tests", 0),
            "cases_latest": item.get("cases", 0),
            "deaths_latest": item.get("deaths", 0),
            "updated_epoch": item.get("updated"),
        }
        for item in resp_live.json()
        if item.get("countryInfo", {}).get("iso3")
    }
    logger.info("Đã lấy Live Summary từ Disease.sh API cho %d quốc gia.", len(disease_sh_summary))

    # 2. Tải trực tiếp luồng dữ liệu chuỗi thời gian từ Our World in Data Live Catalog
    usecols = [
        "code",
        "country",
        "date",
        "new_cases",
        "new_deaths",
        "new_tests",
        "new_tests_smoothed",
        "total_vaccinations",
    ]
    df = pd.read_csv(OWID_LIVE_CSV_URL, usecols=usecols)

    # Lọc theo danh sách quốc gia mục tiêu và khoảng thời gian giám sát
    df_filtered = df[
        (df["code"].isin(TARGET_COUNTRIES))
        & (df["date"] >= START_DATE_FILTER)
        & (df["date"] <= END_DATE_FILTER)
    ].copy()

    # Làm sạch giá trị thiếu (NaN -> 0) và các giá trị âm do hiệu chỉnh thống kê
    df_filtered["new_cases"] = df_filtered["new_cases"].fillna(0).clip(lower=0).astype(int)
    df_filtered["new_deaths"] = df_filtered["new_deaths"].fillna(0).clip(lower=0).astype(int)
    # Nếu new_tests bị trống ở một số ngày, sử dụng new_tests_smoothed thực tế của OWID
    df_filtered["new_tests"] = (
        df_filtered["new_tests"]
        .fillna(df_filtered["new_tests_smoothed"])
        .fillna(0)
        .clip(lower=0)
        .astype(int)
    )
    # Nội suy tiến (forward-fill) cho tổng số mũi tiêm lũy kế theo từng quốc gia
    df_filtered["total_vaccinations"] = (
        df_filtered.groupby("code")["total_vaccinations"]
        .ffill()
        .fillna(0)
        .clip(lower=0)
        .astype("int64")
    )

    owid_payload = {}
    for iso_code, group in df_filtered.groupby("code"):
        records = group[["date", "new_cases", "new_deaths", "new_tests", "total_vaccinations"]].to_dict(
            orient="records"
        )
        owid_payload[iso_code] = {
            "country_name": group["country"].iloc[0],
            "disease_sh_live_summary": disease_sh_summary.get(iso_code, {}),
            "record_count": len(records),
            "data": records,
        }

    with open(output_file, mode="w", encoding="utf-8") as f:
        json.dump(owid_payload, f, ensure_ascii=False, indent=2)

    total_rows = sum(v["record_count"] for v in owid_payload.values())
    logger.info(
        "Hoàn tất trích xuất Live API Nguồn 1: %d quốc gia, %d bản ghi chuỗi thời gian -> %s",
        len(owid_payload),
        total_rows,
        output_file,
    )
    return str(output_file)


def extract_who_csv_callable(**context) -> str:
    """
    Task 2 (Nguồn 2 - File CSV Hạ tầng Y tế từ Live WHO Global Health Observatory API):
    Gọi trực tiếp WHO GHO OData API (ghoapi.azureedge.net) để lấy phân vùng WHO (who_region)
    và số giường bệnh (WHS6_102), kết hợp cùng chỉ số nhân khẩu học (population, gdp,
    beds_per_1000, life_expectancy) từ bộ dữ liệu quốc tế, sau đó xuất ra file CSV chuẩn.
    """
    import pandas as pd
    import requests

    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    target_csv = STAGING_DIR / f"who_health_infrastructure_{execution_date}.csv"

    logger.info("Đang gọi trực tiếp WHO Global Health Observatory API: %s", WHO_GHO_COUNTRY_API)

    # 1. Lấy danh mục Quốc gia & Khu vực WHO (WHO Region) trực tiếp từ WHO GHO API
    resp_countries = requests.get(WHO_GHO_COUNTRY_API, timeout=30)
    resp_countries.raise_for_status()
    who_country_meta = {
        item["Code"]: {
            "who_country_title": item.get("Title"),
            "who_region": item.get("ParentCode", "UNK"),
            "who_region_name": item.get("ParentTitle", "Unknown"),
        }
        for item in resp_countries.json().get("value", [])
        if item.get("Code") in TARGET_COUNTRIES
    }

    # 2. Lấy chỉ số Giường bệnh (WHS6_102: Hospital beds per 10,000 population) trực tiếp từ WHO GHO API
    filter_expr = " or ".join([f"SpatialDim eq '{c}'" for c in TARGET_COUNTRIES])
    resp_beds = requests.get(
        WHO_GHO_BEDS_API,
        params={"$filter": filter_expr},
        timeout=30,
    )
    who_beds_per_1000 = {}
    if resp_beds.ok:
        # Lấy giá trị năm gần nhất của từng quốc gia từ WHO GHO API (chia 10 để đổi từ per 10,000 sang per 1,000)
        beds_rows = sorted(
            resp_beds.json().get("value", []),
            key=lambda x: x.get("TimeDim", 0),
        )
        for row in beds_rows:
            code = row.get("SpatialDim")
            num_val = row.get("NumericValue")
            if code and num_val is not None:
                who_beds_per_1000[code] = round(float(num_val) / 10.0, 3)

    # 3. Trích xuất các chỉ số Dân số, GDP, Tuổi thọ, Châu lục mới nhất từ Live Catalog
    infra_cols = [
        "code",
        "country",
        "continent",
        "population",
        "gdp_per_capita",
        "hospital_beds_per_thousand",
        "life_expectancy",
    ]
    df_infra = pd.read_csv(OWID_LIVE_CSV_URL, usecols=infra_cols)
    df_infra = (
        df_infra[df_infra["code"].isin(TARGET_COUNTRIES)]
        .drop_duplicates(subset=["code"], keep="last")
        .copy()
    )

    continent_code_map = {
        "Asia": "AS",
        "Europe": "EU",
        "North America": "NA",
        "South America": "SA",
        "Africa": "AF",
        "Oceania": "OC",
    }

    csv_rows = []
    for _, row in df_infra.iterrows():
        code = row["code"]
        continent_name = str(row["continent"]) if pd.notna(row["continent"]) else "Unknown"
        continent_code = continent_code_map.get(continent_name, "UN")
        who_meta = who_country_meta.get(code, {})

        population = int(row["population"]) if pd.notna(row["population"]) else 0
        gdp_per_capita = float(row["gdp_per_capita"]) if pd.notna(row["gdp_per_capita"]) else 0.0
        # Ưu tiên số giường bệnh từ WHO GHO API (WHS6_102), nếu thiếu thì lấy từ catalog
        beds = who_beds_per_1000.get(
            code,
            round(float(row["hospital_beds_per_thousand"]), 3)
            if pd.notna(row["hospital_beds_per_thousand"])
            else 0.0,
        )
        life_exp = round(float(row["life_expectancy"]), 2) if pd.notna(row["life_expectancy"]) else 0.0

        csv_rows.append(
            {
                "country_code": code,
                "country_name": who_meta.get("who_country_title") or row["country"],
                "continent_code": continent_code,
                "continent_name": continent_name,
                "who_region": who_meta.get("who_region", "UNK"),
                "population": population,
                "gdp": round(gdp_per_capita, 2),
                "beds_per_1000": beds,
                "life_expectancy": life_exp,
            }
        )

    fieldnames = [
        "country_code",
        "country_name",
        "continent_code",
        "continent_name",
        "who_region",
        "population",
        "gdp",
        "beds_per_1000",
        "life_expectancy",
    ]

    # Ghi ra file staging và cập nhật luôn file CSV trong thư mục data/raw
    for path in [target_csv, Path(WHO_CSV_OUTPUT_PATH)]:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)

    logger.info(
        "Đã tải dữ liệu thực tế từ WHO GHO API cho %d quốc gia và lưu CSV tại: %s",
        len(csv_rows),
        target_csv,
    )
    return str(target_csv)


def extract_mysql_oltp_callable(**context) -> str:
    """
    Task 3 (Nguồn 3 - CSDL Relational DB OLTP):
    Truy vấn trực tiếp CSDL MySQL OLTP (vaccination_oltp) để lấy dữ liệu vận hành:
      - Danh sách bệnh nhân & khai báo y tế (bảng patients)
      - Lịch sử tiêm chủng nội bộ (bảng vaccination_records)
    """
    import pymysql

    execution_date = context["ds_nodash"]
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    output_file = STAGING_DIR / f"mysql_oltp_snapshot_{execution_date}.json"

    logger.info("Đang truy vấn trực tiếp Nguồn 3 (MySQL OLTP) tại %s:%s/%s", OLTP_HOST, OLTP_PORT, OLTP_DB)
    conn = pymysql.connect(
        host=OLTP_HOST,
        port=OLTP_PORT,
        user=OLTP_USER,
        password=OLTP_PASSWORD,
        database=OLTP_DB,
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    country_code,
                    DATE_FORMAT(reported_date, '%Y-%m-%d') AS report_date,
                    COUNT(*) AS patients_declared,
                    SUM(CASE WHEN health_status = 'CONFIRMED_COVID' THEN 1 ELSE 0 END) AS confirmed_patients
                FROM patients
                GROUP BY country_code, DATE_FORMAT(reported_date, '%Y-%m-%d');
                """
            )
            patient_stats = cur.fetchall()

            cur.execute(
                """
                SELECT
                    country_code,
                    DATE_FORMAT(vaccination_date, '%Y-%m-%d') AS vax_date,
                    COUNT(*) AS daily_vax_doses
                FROM vaccination_records
                GROUP BY country_code, DATE_FORMAT(vaccination_date, '%Y-%m-%d');
                """
            )
            vax_stats = cur.fetchall()

        oltp_payload = {
            "extracted_at": datetime.utcnow().isoformat(),
            "patient_declarations": patient_stats,
            "vaccination_history": vax_stats,
        }

        with open(output_file, mode="w", encoding="utf-8") as f:
            json.dump(oltp_payload, f, ensure_ascii=False, indent=2, default=str)

        logger.info("Đã trích xuất Nguồn 3 (MySQL OLTP) thành công: %s", output_file)
        return str(output_file)
    finally:
        conn.close()


def load_to_minio_callable(**context) -> dict:
    """
    Task 4: Đẩy cả 3 nguồn dữ liệu thô vừa tải trực tiếp lên MinIO Data Lake.
    """
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    ti = context["ti"]
    execution_date = context["ds"]

    owid_json_path = ti.xcom_pull(task_ids="extract_owid_api")
    who_csv_path = ti.xcom_pull(task_ids="extract_who_csv")
    oltp_json_path = ti.xcom_pull(task_ids="extract_mysql_oltp")

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

    s3_keys = {
        "owid_key": f"raw/owid/{execution_date}/owid_timeseries.json",
        "who_key": f"raw/who/{execution_date}/who_health_infrastructure.csv",
        "oltp_key": f"raw/oltp/{execution_date}/mysql_oltp_snapshot.json",
    }

    s3_client.upload_file(owid_json_path, MINIO_BUCKET, s3_keys["owid_key"])
    s3_client.upload_file(who_csv_path, MINIO_BUCKET, s3_keys["who_key"])
    s3_client.upload_file(oltp_json_path, MINIO_BUCKET, s3_keys["oltp_key"])

    logger.info("Đã đẩy đủ 3 nguồn dữ liệu thực tế lên MinIO bucket '%s': %s", MINIO_BUCKET, s3_keys)
    return {"bucket": MINIO_BUCKET, **s3_keys}


def transform_and_load_to_dwh_callable(**context) -> None:
    """
    Task 5: Đọc trực tiếp cả 3 file từ MinIO Data Lake (s3://covid19-landing-zone/...),
    chuẩn hóa và nạp (Upsert) vào Snowflake Schema trên PostgreSQL DWH.
    """
    import boto3
    import psycopg2
    from botocore.client import Config

    ti = context["ti"]
    minio_meta = ti.xcom_pull(task_ids="load_to_minio")
    bucket = minio_meta["bucket"]

    # Kết nối MinIO để đọc trực tiếp dữ liệu từ tầng Data Lake (Landing Zone)
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    owid_obj = s3_client.get_object(Bucket=bucket, Key=minio_meta["owid_key"])
    owid_data = json.loads(owid_obj["Body"].read().decode("utf-8"))

    who_obj = s3_client.get_object(Bucket=bucket, Key=minio_meta["who_key"])
    who_csv_text = who_obj["Body"].read().decode("utf-8")

    oltp_obj = s3_client.get_object(Bucket=bucket, Key=minio_meta["oltp_key"])
    oltp_data = json.loads(oltp_obj["Body"].read().decode("utf-8"))

    oltp_patients_lookup = {
        (row["country_code"], row["report_date"]): int(row["patients_declared"] or 0)
        for row in oltp_data.get("patient_declarations", [])
    }
    oltp_vax_lookup = {
        (row["country_code"], row["vax_date"]): int(row["daily_vax_doses"] or 0)
        for row in oltp_data.get("vaccination_history", [])
    }

    conn = psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE dwh.dim_country ADD COLUMN IF NOT EXISTS life_expectancy NUMERIC(5,2);")
            cur.execute("ALTER TABLE dwh.fact_cases ADD COLUMN IF NOT EXISTS new_tests INT DEFAULT 0;")
            cur.execute("ALTER TABLE dwh.fact_cases ADD COLUMN IF NOT EXISTS internal_vax_doses INT DEFAULT 0;")
            cur.execute("ALTER TABLE dwh.fact_cases ADD COLUMN IF NOT EXISTS internal_patients_declared INT DEFAULT 0;")

            cur.execute(
                """
                INSERT INTO dwh.dim_indicator (indicator_code, description)
                VALUES (
                    'LIVE_MULTI_SOURCE_COVID19',
                    'Dữ liệu thực tế từ Live OWID/Disease.sh API + Live WHO GHO API + MySQL OLTP'
                )
                ON CONFLICT (indicator_code) DO UPDATE SET description = EXCLUDED.description
                RETURNING id;
                """
            )
            indicator_id = cur.fetchone()[0]

            # 1. Nạp Nguồn 2 (WHO GHO CSV từ MinIO) vào dim_region và dim_country
            country_id_map = {}
            reader = csv.DictReader(io.StringIO(who_csv_text))
            for row in reader:
                cur.execute(
                    """
                    INSERT INTO dwh.dim_region (continent_code, continent_name, who_region)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (continent_code) DO UPDATE SET
                        continent_name = EXCLUDED.continent_name,
                        who_region = EXCLUDED.who_region
                    RETURNING id;
                    """,
                    (row["continent_code"], row["continent_name"], row["who_region"]),
                )
                region_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO dwh.dim_country (
                        country_code, country_name, population, gdp,
                        beds_per_1000, life_expectancy, region_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (country_code) DO UPDATE SET
                        country_name = EXCLUDED.country_name,
                        population = EXCLUDED.population,
                        gdp = EXCLUDED.gdp,
                        beds_per_1000 = EXCLUDED.beds_per_1000,
                        life_expectancy = EXCLUDED.life_expectancy,
                        region_id = EXCLUDED.region_id,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                    """,
                    (
                        row["country_code"],
                        row["country_name"],
                        int(row["population"]),
                        float(row["gdp"]),
                        float(row["beds_per_1000"]),
                        float(row["life_expectancy"]),
                        region_id,
                    ),
                )
                country_id_map[row["country_code"]] = cur.fetchone()[0]

            # 2. Nạp Nguồn 1 (OWID Live Time-series) + Nguồn 3 (MySQL OLTP) vào dim_date & fact_cases
            fact_rows_upserted = 0
            for country_code, payload in owid_data.items():
                country_id = country_id_map.get(country_code)
                if not country_id:
                    continue

                for record in payload.get("data", []):
                    date_str = record["date"]
                    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                    date_id = int(dt.strftime("%Y%m%d"))
                    is_weekend = dt.isoweekday() in (6, 7)

                    cur.execute(
                        """
                        INSERT INTO dwh.dim_date (date_id, full_date, year, month, day, is_weekend)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date_id) DO NOTHING;
                        """,
                        (date_id, dt, dt.year, dt.month, dt.day, is_weekend),
                    )

                    internal_vax = oltp_vax_lookup.get((country_code, date_str), 0)
                    internal_patients = oltp_patients_lookup.get((country_code, date_str), 0)

                    cur.execute(
                        """
                        INSERT INTO dwh.fact_cases (
                            country_id, date_id, indicator_id,
                            new_cases, new_deaths, new_tests,
                            total_vaccinations, internal_vax_doses, internal_patients_declared
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_id, date_id) DO UPDATE SET
                            indicator_id = EXCLUDED.indicator_id,
                            new_cases = EXCLUDED.new_cases,
                            new_deaths = EXCLUDED.new_deaths,
                            new_tests = EXCLUDED.new_tests,
                            total_vaccinations = EXCLUDED.total_vaccinations,
                            internal_vax_doses = EXCLUDED.internal_vax_doses,
                            internal_patients_declared = EXCLUDED.internal_patients_declared;
                        """,
                        (
                            country_id,
                            date_id,
                            indicator_id,
                            int(record.get("new_cases", 0)),
                            int(record.get("new_deaths", 0)),
                            int(record.get("new_tests", 0)),
                            int(record.get("total_vaccinations", 0)) + internal_vax,
                            internal_vax,
                            internal_patients,
                        ),
                    )
                    fact_rows_upserted += 1

        conn.commit()
        logger.info(
            "Đã đọc từ MinIO và nạp thành công %d bản ghi thực tế vào dwh.fact_cases!",
            fact_rows_upserted,
        )
    except Exception as exc:
        conn.rollback()
        logger.error("Lỗi khi nạp dữ liệu vào DWH: %s", exc)
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
    description="Live Multi-Source Public Health Pipeline: Live OWID/Disease.sh API + Live WHO GHO API + MySQL OLTP -> MinIO -> Postgres DWH",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["public_health", "live_api", "owid", "who_gho", "mysql_oltp", "minio", "snowflake_dwh"],
) as dag:

    extract_owid_api = PythonOperator(
        task_id="extract_owid_api",
        python_callable=extract_owid_api_callable,
        doc_md="Nguồn 1 (Live API): Gọi trực tiếp Our World in Data Catalog & Disease.sh API.",
    )

    extract_who_csv = PythonOperator(
        task_id="extract_who_csv",
        python_callable=extract_who_csv_callable,
        doc_md="Nguồn 2 (Live WHO GHO API): Gọi trực tiếp ghoapi.azureedge.net lấy chỉ số giường bệnh & phân vùng WHO xuất ra CSV.",
    )

    extract_mysql_oltp = PythonOperator(
        task_id="extract_mysql_oltp",
        python_callable=extract_mysql_oltp_callable,
        doc_md="Nguồn 3 (MySQL OLTP): Truy vấn trực tiếp bảng patients & vaccination_records.",
    )

    load_to_minio = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio_callable,
        doc_md="Đẩy cả 3 nguồn dữ liệu thực tế lên MinIO bucket covid19-landing-zone.",
    )

    transform_and_load_to_dwh = PythonOperator(
        task_id="transform_and_load_to_dwh",
        python_callable=transform_and_load_to_dwh_callable,
        doc_md="Đọc trực tiếp 3 nguồn từ MinIO và Upsert vào PostgreSQL DWH.",
    )

    [extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh
