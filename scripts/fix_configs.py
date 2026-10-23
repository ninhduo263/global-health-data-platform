import re

# ==========================================
# 1. Update docker-compose.yml
# ==========================================
with open("docker-compose.yml", "r", encoding="utf-8") as f:
    dc_text = f.read().replace("\r\n", "\n")

# Update Airflow env
old_env = """    # Cài đặt thêm thư viện giao tiếp với MinIO (boto3) và PostgreSQL (psycopg2)
    _PIP_ADDITIONAL_REQUIREMENTS: "boto3 psycopg2-binary requests pandas"
    # Biến môi trường truyền vào DAG
    MINIO_ENDPOINT: "http://minio:9000"
    MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin123}
    MINIO_BUCKET_NAME: ${MINIO_BUCKET_NAME:-covid19-landing-zone}
    DWH_HOST: "postgres-dwh"
    DWH_PORT: "5432"
    DWH_DB: ${POSTGRES_DB:-health_dwh}
    DWH_USER: ${POSTGRES_USER:-dwh_admin}
    DWH_PASSWORD: ${POSTGRES_PASSWORD:-dwh_password}"""

new_env = """    # Cài đặt thêm thư viện giao tiếp với MinIO (boto3), PostgreSQL (psycopg2) và MySQL OLTP (pymysql)
    _PIP_ADDITIONAL_REQUIREMENTS: "boto3 psycopg2-binary pymysql requests pandas"
    # Biến môi trường truyền vào DAG
    MINIO_ENDPOINT: "http://minio:9000"
    MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin123}
    MINIO_BUCKET_NAME: ${MINIO_BUCKET_NAME:-covid19-landing-zone}
    DWH_HOST: "postgres-dwh"
    DWH_PORT: "5432"
    DWH_DB: ${POSTGRES_DB:-health_dwh}
    DWH_USER: ${POSTGRES_USER:-dwh_admin}
    DWH_PASSWORD: ${POSTGRES_PASSWORD:-dwh_password}
    OLTP_HOST: "mysql-oltp"
    OLTP_PORT: "3306"
    OLTP_DB: ${MYSQL_DATABASE:-vaccination_oltp}
    OLTP_USER: ${MYSQL_USER:-oltp_user}
    OLTP_PASSWORD: ${MYSQL_PASSWORD:-oltp_password}"""

if old_env in dc_text:
    dc_text = dc_text.replace(old_env, new_env)
    print("Updated Airflow environment in docker-compose.yml")
else:
    print("Airflow env already updated or not found")

# Fix Superset multi-line command
old_superset_cmd = """    command: >
      /bin/sh -c "
      pip install psycopg2-binary &&
      superset db upgrade &&
      superset fab create-admin
        --username $${SUPERSET_ADMIN_USER}
        --firstname Superset
        --lastname Admin
        --email $${SUPERSET_ADMIN_EMAIL}
        --password $${SUPERSET_ADMIN_PASSWORD} &&
      superset init &&
      /usr/bin/run-server.sh
      " """

# Also match pattern with regex
pattern = r"command:\s*>\s*/bin/sh -c \".*?superset fab create-admin.*?/usr/bin/run-server\.sh\s*\""
replacement = """command:
      - /bin/sh
      - -c
      - |
        pip install psycopg2-binary &&
        superset db upgrade &&
        superset fab create-admin --username "$${SUPERSET_ADMIN_USER}" --firstname Superset --lastname Admin --email "$${SUPERSET_ADMIN_EMAIL}" --password "$${SUPERSET_ADMIN_PASSWORD}" &&
        superset init &&
        /usr/bin/run-server.sh"""

dc_text, n = re.subn(pattern, replacement, dc_text, flags=re.DOTALL)
print(f"Replaced superset command: {n} match(es)")

with open("docker-compose.yml", "w", encoding="utf-8") as f:
    f.write(dc_text)

# ==========================================
# 2. Update airflow/dags/health_data_pipeline.py
# ==========================================
with open("airflow/dags/health_data_pipeline.py", "r", encoding="utf-8") as f:
    dag_text = f.read().replace("\r\n", "\n")

# Ensure OWID_LIVE_CSV_URL is defined
if "OWID_LIVE_CSV_URL" not in dag_text:
    dag_text = dag_text.replace(
        'OWID_API_URL = os.getenv(\n    "OWID_API_URL",\n',
        'OWID_API_URL = os.getenv(\n    "OWID_API_URL",\n'
    )

# At the top of config, ensure:
old_cfg = """OWID_API_URL = os.getenv(
    "OWID_API_URL",
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
)"""

new_cfg = """OWID_API_URL = os.getenv(
    "OWID_API_URL",
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
)
OWID_LIVE_CSV_URL = os.getenv(
    "OWID_LIVE_CSV_URL",
    "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv",
)"""

if old_cfg in dag_text:
    dag_text = dag_text.replace(old_cfg, new_cfg)
    print("Added OWID_LIVE_CSV_URL to health_data_pipeline.py")

with open("airflow/dags/health_data_pipeline.py", "w", encoding="utf-8") as f:
    f.write(dag_text)

print("Done fixing configurations!")

