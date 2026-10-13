# Public Health Data Platform (COVID-19 / Bệnh truyền nhiễm)

Hệ thống Data Platform cục bộ chạy trên Docker Compose (Windows WSL2) phục vụ thu thập, đồng bộ CDC, lưu trữ Data Lake (MinIO), điều phối ETL (Apache Airflow), kho dữ liệu phân tích (PostgreSQL Snowflake Schema) và trực quan hóa BI (Apache Superset).

## 1. Bảng ánh xạ cổng & Thông tin đăng nhập (Windows Host)

| Dịch vụ | Container Name | Địa chỉ truy cập (Host) | Tài khoản mặc định | Ghi chú |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL (DWH)** | `postgres-dwh` | `localhost:5433` | `dwh_admin` / `dwh_password` | Database: `health_dwh` (Schema: `dwh`) |
| **MySQL (OLTP)** | `mysql-oltp` | `localhost:3306` | `root` / `root_password` | Database: `vaccination_oltp` (`oltp_user` / `oltp_password`) |
| **MinIO API** | `minio` | `http://localhost:9000` | `minioadmin` / `minioadmin123` | S3-compatible endpoint |
| **MinIO Console** | `minio` | `http://localhost:9001` | `minioadmin` / `minioadmin123` | Bucket mặc định: `covid19-landing-zone` |
| **Airflow Webserver** | `airflow-webserver` | `http://localhost:8081` | `admin` / `admin` | DAG: `health_data_pipeline` |
| **Apache Superset** | `superset` | `http://localhost:8089` | `admin` / `admin` | SQLAlchemy URI kết nối DWH: `postgresql+psycopg2://dwh_admin:dwh_password@postgres-dwh:5432/health_dwh` |
| **Debezium Connect** | `debezium` | `http://localhost:8083` | *(REST API)* | Đăng ký connector từ `config/debezium/register-mysql-connector.json` |
| **Kafka Broker** | `kafka` | `localhost:9092` | - | Internal: `kafka:9092` |

## 2. Hướng dẫn khởi chạy trên Windows

```powershell
# 1. Khởi tạo toàn bộ các container chạy ngầm
docker compose up -d

# 2. Kiểm tra trạng thái các container
docker compose ps

# 3. Đăng ký Debezium MySQL CDC Connector (sau khi debezium đã khởi động xong)
Invoke-RestMethod -Uri "http://localhost:8083/connectors/" `
  -Method Post `
  -ContentType "application/json" `
  -InFile "config/debezium/register-mysql-connector.json"
```

