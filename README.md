# Public Health Data Platform (COVID-19 / Bệnh truyền nhiễm)

Hệ thống Data Platform cục bộ chạy trên Docker Compose (Windows WSL2) phục vụ thu thập, đồng bộ CDC, lưu trữ Data Lake (MinIO), điều phối ETL (Apache Airflow), kho dữ liệu phân tích (PostgreSQL Snowflake Schema) và trực quan hóa BI (Apache Superset).

## 1. Bảng ánh xạ cổng & Thông tin đăng nhập (Windows Host)

| Dịch vụ | Container Name | Địa chỉ truy cập (Host) | Tài khoản mặc định | Ghi chú |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL (DWH)** | `postgres-dwh` | `localhost:5433` | `dwh_admin` / `dwh_password` | Database: `health_dwh` (Schema: `dwh`) |
| **MySQL (OLTP)** | `mysql-oltp` | `localhost:3307` | `root` / `root_password` | Database: `vaccination_oltp` (`oltp_user` / `oltp_password`) |
| **MinIO API** | `minio` | `http://localhost:9000` | `minioadmin` / `minioadmin123` | S3-compatible endpoint |
| **MinIO Console** | `minio` | `http://localhost:9001` | `minioadmin` / `minioadmin123` | Bucket mặc định: `covid19-landing-zone` |
| **Airflow Webserver** | `airflow-webserver` | `http://localhost:8081` | `admin` / `admin` | DAG: `health_data_pipeline`; dùng cookie riêng `airflow_session` |
| **Apache Superset** | `superset` | `http://localhost:8089` | `admin` / `admin` | SQLAlchemy URI kết nối DWH: `postgresql+psycopg2://dwh_admin:dwh_password@postgres-dwh:5432/health_dwh` |
| **Debezium Connect** | `debezium` | `http://localhost:8083` | *(REST API)* | Đăng ký connector từ `config/debezium/register-mysql-connector.json` |
| **Kafka Broker** | `kafka` | `localhost:9092` | - | Internal: `kafka:9092` |

## 2. Khởi chạy trên Windows

```powershell
# Chạy tại thư mục global-health-data-platform
docker compose up -d

docker compose ps
```

Compose tự chạy migration DWH, đăng ký Debezium connector và bật CDC consumer. Lần chạy đầu có thể mất vài phút để tải image và khởi tạo Airflow/Superset.
Pipeline WHO đọc `data/raw/WHO-COVID-19-global-daily-data.csv`, lọc 15 mã
`VN,US,GB,DE,FR,IT,ES,IN,CN,JP,KR,BR,RU,AU,CA` trong giai đoạn
`2020-01-01` đến `2023-12-31`. Mã Việt Nam trong file WHO là `VN` (ISO-2);
OWID và CDC dùng `VNM` (ISO-3). WHO và OWID được giữ ở các fact/view riêng, không join trực tiếp bằng mã quốc gia.

Airflow dùng cookie phiên riêng vì cookie trình duyệt không phân biệt port; nếu
đã mở Airflow trước khi đổi cấu hình, hãy recreate webserver rồi mở lại
`http://localhost:8081` trong cửa sổ ẩn danh hoặc xoá cookie của `localhost`.

```powershell
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

## 3. Các luồng dữ liệu

- DAG `health_data_pipeline` tải snapshot OWID để nạp dữ liệu COVID-19 ISO-3 theo quốc gia/ngày vào `dwh.fact_cases`; đồng thời lọc WHO theo danh sách quốc gia và giai đoạn cấu hình, lưu raw lên MinIO rồi nạp số liệu WHO ISO-2 vào `dwh.fact_who_covid_daily`. Hai bảng `vaccination_centers` / `vaccination_records` của MySQL cũng được lưu raw vào MinIO.
- Nguồn OWID gồm các quốc gia và vùng lãnh thổ có mã quốc gia 3 ký tự, với các trường hợp mắc/tử vong mới, tổng lũy kế và số liệu tiêm chủng. Dữ liệu lịch sử có sẵn từ năm 2020; ngày cuối cùng phụ thuộc snapshot OWID hiện hành (không phải luồng thời gian thực).
- Debezium phát CDC cho hai bảng MySQL vào Kafka. Connector sử dụng tài khoản replication `oltp_user` (không dùng root); quyền replication được cấp trong `scripts/sql/init-oltp.sql`. Service `cdc-consumer` tự đăng ký/cập nhật connector; connector dùng `snapshot.mode=when_needed` để chụp snapshot ban đầu hoặc chụp lại khi binlog/offset cũ không còn khả dụng (ví dụ sau khi MySQL được tạo lại), rồi tiếp tục theo dõi thay đổi. Consumer upsert/xóa dữ liệu trong `dwh.dim_vaccination_center` và `dwh.fact_vaccination`. Giao dịch được xử lý idempotent; Kafka offset chỉ được commit sau khi transaction DWH thành công.
- Migration `scripts/sql/upgrade-dwh.sql` chạy an toàn nhiều lần và tạo các bảng CDC cùng view phục vụ BI. View cũng được tạo mới khi PostgreSQL khởi tạo volume lần đầu.
- MySQL dùng cổng host `3307` vì host đã có dịch vụ chiếm `3306`; các container vẫn kết nối nội bộ qua cổng `3306`.

## 4. Tạo dashboard Superset

Compose tự động tạo database connection **Health DWH** trong Superset với PostgreSQL DWH
(`postgres-dwh:5432/health_dwh`). Vào **Data → Databases → Health DWH → Explore** để
kiểm tra các bảng/view trong schema `dwh`. Nếu connection đã tồn tại từ lần chạy trước,
Compose sẽ cập nhật lại URI để tránh trỏ nhầm `localhost` hoặc database SQLite metadata.

Kết nối database trong Superset bằng PostgreSQL:

| Trường | Giá trị |
|---|---|
| Host | `postgres-dwh` |
| Port | `5432` |
| Database | `health_dwh` |
| Username / Password | `dwh_admin` / `dwh_password` |

Trong Superset, vào **Data → Datasets → + Dataset**, chọn database trên, schema `dwh`, rồi thêm các view:

- `vw_dashboard_cases` — dữ liệu ca mắc/tử vong và tiêm chủng OWID theo ngày, quốc gia, khu vực.
- `vw_who_covid_daily` — dữ liệu WHO theo ngày, mã ISO-2 và WHO region.
- `vw_dashboard_vaccinations` — số lượt tiêm theo ngày, trung tâm, vaccine và số mũi.
- `vw_weekly_covid_clusters` — kết quả K-means theo quốc gia/tuần.

Task `weekly_covid_kmeans` chạy sau khi OWID được nạp vào DWH. Task tổng hợp ca mắc/tử vong theo tuần, chuẩn hóa theo dân số, rồi phân cụm theo incidence, mortality và biến động ca so với tuần trước. Mặc định có 4 cụm, cấu hình qua `COVID_KMEANS_CLUSTERS`; kết quả upsert vào `dwh.fact_weekly_covid_cluster`. `cluster_id` được sắp theo mức ca mắc trung bình trên 100.000 dân trong lần chạy. Đây là phân cụm xu hướng, không phải dự báo/cảnh báo tự động.


Nếu database volume đã tồn tại trước khi thêm bảng phân cụm, chạy lại migration và khởi động lại Airflow để cài dependency mới:

```powershell
docker compose up -d --force-recreate dwh-migrate
docker compose logs --tail 30 dwh-migrate
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Để tạo dashboard, thêm dataset `vw_weekly_covid_clusters`, đồng bộ cột sau lần chạy DAG đầu tiên, rồi có thể tạo:

1. **Số quốc gia theo cụm và tuần:** Bar Chart, X-axis `cluster_id`, metric `COUNT_DISTINCT(country_code)`, time column `week_start`.
2. **Incidence theo tuần:** Time-series Line Chart, X-axis `week_start`, metric `AVG(cases_per_100k)`, dimension `cluster_id`.
3. **Chi tiết quốc gia:** Table với `week_start`, `country_code`, `country_name`, `cluster_id`, `cases_per_100k`, `deaths_per_100k`, `case_growth_pct`.

Với dataset `vw_who_covid_daily`, có thể tạo line chart ca mắc mới theo ngày (X-axis `report_date`, metric `SUM(new_cases)`, dimension `who_region`) và chart tử vong mới tương tự với `SUM(new_deaths)`. WHO dùng ISO-2, nên không join trực tiếp `country_code` của view này với mã ISO-3 trong `vw_dashboard_cases`.

Sau lần nạp OWID, mở dataset `vw_dashboard_cases` trong Superset và đồng bộ/refresh danh sách cột để nhận các trường mới (`new_vaccinations`, `people_fully_vaccinated`, `total_cases`, `total_deaths`). Hai chart tiêm chủng đang có trong dashboard tạo từ CDC MySQL vẫn chỉ là dữ liệu mẫu nội bộ; muốn dashboard cũng thể hiện quy mô toàn cầu thì sửa dataset của chúng sang `vw_dashboard_cases`:

1. **Ca mắc theo ngày và châu lục:** Time-series Line Chart, X-axis `report_date`, metric `SUM(new_cases)`, dimension `continent_name`.
2. **Tử vong theo ngày và châu lục:** Time-series Line Chart, X-axis `report_date`, metric `SUM(new_deaths)`, dimension `continent_name`.
3. **Số liều tiêm mới toàn cầu theo ngày:** Time-series Line Chart, X-axis `report_date`, metric `SUM(new_vaccinations)`.
4. **Số người đã tiêm đầy đủ theo châu lục/ngày:** Time-series Line Chart, X-axis `report_date`, metric `SUM(people_fully_vaccinated)`, dimension `continent_name`. Cộng các quốc gia trong cùng một ngày; không cộng lũy kế qua nhiều ngày.

Lưu từng chart, thêm chúng vào dashboard mới rồi nhấn **Save**. Để nạp snapshot OWID toàn cầu vào DWH lần đầu hoặc làm mới thủ công, vào Airflow `http://localhost:8081`, mở DAG `health_data_pipeline` và chọn **Trigger DAG**. DAG sẽ tải lại snapshot, lưu bản thô vào MinIO và upsert idempotent theo quốc gia/ngày. Có thể kiểm tra khối lượng bằng truy vấn SQL:

```sql
SELECT COUNT(*) AS observations,
       COUNT(DISTINCT country_code) AS countries,
       MIN(report_date) AS first_date,
       MAX(report_date) AS last_date
FROM dwh.vw_dashboard_cases;
```

Luồng vaccine nội bộ MySQL là dữ liệu minh họa riêng; nó không đại diện cho tiêm chủng toàn cầu. Nếu view tiêm chủng nội bộ chưa có hàng, hãy kiểm tra trạng thái `cdc-consumer` và Debezium connector:

```powershell
docker compose logs --tail 50 cdc-consumer
curl.exe http://localhost:8083/connectors/vaccination-oltp-connector/status
```

CDC consumer không đưa `citizen_hash` vào DWH; dashboard chỉ chứa số liệu tổng hợp. DAG lưu bản raw MySQL vào MinIO, không thay thế luồng CDC.
