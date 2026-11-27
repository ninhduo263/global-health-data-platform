# Public Health Data Platform & Data Mining (COVID-19 / Bệnh truyền nhiễm)

Hệ thống Data Platform & Data Mining cục bộ chạy trên Docker Compose (Windows WSL2) phục vụ thu thập đa nguồn, đồng bộ CDC ẩn danh, lưu trữ Data Lake (MinIO), điều phối ETL & ML pipeline (Apache Airflow), kho dữ liệu phân tích (PostgreSQL Snowflake Schema), phân cụm dịch tễ K-Means nâng cao, cảnh báo sớm bùng phát dịch bằng XGBoost và trực quan hóa BI (Apache Superset).

> **Tài liệu liên quan:** [Giới thiệu tổng quan hệ thống & thuật toán (GIOI_THIEU_HE_THONG.md)](./GIOI_THIEU_HE_THONG.md) | [Kịch bản bảo vệ & hỏi đáp (DEMO.md)](./DEMO.md)

## 1. Bảng ánh xạ cổng & Thông tin đăng nhập (Windows Host)

| Dịch vụ | Container Name | Địa chỉ truy cập (Host) | Tài khoản mặc định | Ghi chú |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL (DWH)** | `postgres-dwh` | `localhost:5433` | `dwh_admin` / `dwh_password` | Database: `health_dwh` (Schema: `dwh`) |
| **MySQL (OLTP)** | `mysql-oltp` | `localhost:3307` | `root` / `root_password` | Database: `vaccination_oltp` (`oltp_user` / `oltp_password`) |
| **MinIO API** | `minio` | `http://localhost:9000` | `minioadmin` / `minioadmin123` | S3-compatible endpoint |
| **MinIO Console** | `minio` | `http://localhost:9001` | `minioadmin` / `minioadmin123` | Bucket mặc định: `covid19-landing-zone` |
| **Airflow Webserver** | `airflow-webserver` | `http://localhost:8081` | `admin` / `admin` | DAG: `health_data_pipeline`; dùng cookie riêng `airflow_session` |
| **Apache Superset** | `superset` | `http://localhost:8089` | `admin` / `admin` | SQLAlchemy URI kết nối DWH: `postgresql+psycopg2://dwh_admin:dwh_password@postgres-dwh:5432/health_dwh` |
| **AI Interactive Portal** | `ai-portal` | `http://localhost:8501` | *(Mở trực tiếp)* | Cổng Streamlit: Mô phỏng What-If XGBoost + TreeSHAP, Tra cứu Apriori, Markov $P^k$ & Cảnh báo ổ dịch Realtime |
| **Debezium Connect** | `debezium` | `http://localhost:8083` | *(REST API)* | Connector `vaccination-oltp-connector-v2` từ `config/debezium/register-mysql-connector.json` |
| **Kafka Broker** | `kafka` | `localhost:9092` | - | Internal: `kafka:9092` |

## 2. Khởi chạy trên Windows

```powershell
docker compose up -d --build
docker compose ps
```

Nếu cập nhật từ database volume cũ, chạy lại migration (idempotent) và recreate Airflow / CDC consumer:

```powershell
docker compose up -d --force-recreate dwh-migrate cdc-consumer airflow-webserver airflow-scheduler
docker compose logs --tail 30 dwh-migrate
```

Để sinh thêm vài nghìn bệnh nhân và mũi tiêm giả lập vào MySQL OLTP phục vụ biểu đồ nhân khẩu học/bệnh nền qua CDC:

```powershell
python .\scripts\seed_oltp_data.py --patients 2500
```

## 3. Các luồng dữ liệu & mô hình Data Mining

### 3.1. Batch ETL & Data Lake (`health_data_pipeline`)
- `extract_owid_api` tải snapshot lịch sử toàn cầu OWID (ISO-3), `extract_who_csv` lọc dữ liệu WHO (15 mã ISO-2 giai đoạn `2020-01-01` đến `2023-12-31`), `extract_mysql_oltp` xuất raw 3 bảng OLTP (`vaccination_centers`, `patients` đã lược bỏ `full_name`, `vaccination_records`) lên MinIO (`covid19-landing-zone`).
- `transform_and_load_to_dwh` nạp OWID vào `dwh.dim_region`, `dwh.dim_country`, `dwh.dim_date`, `dwh.fact_cases` và nạp WHO vào bảng riêng `dwh.fact_who_covid_daily`.

### 3.2. Real-time CDC & Ẩn danh hóa dữ liệu bệnh nhân
- Debezium connector `vaccination-oltp-connector-v2` capture 3 bảng MySQL (`vaccination_centers`, `patients`, `vaccination_records`) qua binlog vào Kafka.
- Service `cdc-consumer` tự động xóa connector cũ (`DEBEZIUM_LEGACY_CONNECTORS`) để tránh trùng sự kiện, đồng bộ idempotent vào `dwh.dim_vaccination_center`, `dwh.dim_patient` và `dwh.fact_vaccination`.
- **Bảo vệ quyền riêng tư (Data Minimization)**: `dwh.dim_patient` chỉ lưu `birth_year`, `gender`, `province`, `health_status`, `comorbidity`, `reported_date_id`; tuyệt đối **không** sao chép `full_name` hay `citizen_hash` sang DWH. Hỗ trợ bản ghi `is_placeholder` khi event mũi tiêm tới trước event bệnh nhân.

### 3.3. Phân cụm dịch tễ K-Means nâng cao (`weekly_covid_kmeans`)
- Đặc trưng chuẩn hóa (`StandardScaler`): `log1p(cases_per_100k)`, `log1p(deaths_per_100k)` và biến động `log` ca mắc so với tuần trước.
- Đánh giá dải `K` (`COVID_KMEANS_K_RANGE`, mặc định `2-8`) trên 4 chỉ số: **Inertia (Elbow)**, **Silhouette** (trên mẫu tối đa 10.000 dòng), **Davies–Bouldin** và **Calinski–Harabasz**, lưu vào `dwh.fact_kmeans_evaluation`.
- Hỗ trợ chọn `COVID_KMEANS_CLUSTERS=4` cố định hoặc `COVID_KMEANS_CLUSTERS=auto` (tự chọn `K` có Silhouette cao nhất).
- Tự sinh hồ sơ cụm (`dwh.fact_kmeans_cluster_profile`) kèm nhãn tiếng Việt theo mức ca mắc và xu hướng tâm cụm (ví dụ: *“Cao – đang tăng”*, *“Thấp – ổn định”*) và chiếu **PCA 2 chiều** (`pca_1`, `pca_2`) trong `dwh.fact_weekly_covid_cluster`.

### 3.4. Cảnh báo sớm bùng phát dịch bằng XGBoost (`covid_early_warning`)
- Bài toán phân lớp nhị phân ở mức `(quốc gia, tuần t)`: dự đoán sau `H` tuần (`COVID_EW_HORIZON_WEEKS=2`), tỷ lệ ca mắc có tăng gấp `>= COVID_EW_SURGE_RATIO` (1.5x) và vượt ngưỡng tối thiểu `COVID_EW_MIN_CASES_PER_100K` (10 ca/100k dân) hay không.
- Bộ 18 đặc trưng nhân quả (chỉ dùng dữ liệu `<= t`): mức ca & lag 1–3 tuần, trung bình/độ lệch chuẩn trượt 4 tuần, tốc độ tăng 1–2 tuần & gia tốc, tử vong & CFR trượt 4 tuần, độ phủ tiêm chủng `>=1` mũi & tiêm đủ (`cummax / population`), mùa vụ (`sin`/`cos` tuần trong năm), tốc độ tăng trung bình toàn cầu cùng tuần và `cluster_id` từ K-Means.
- Chia tập train/test theo thời gian có **khoảng trống purge `H` tuần** (`train < cutoff - H`, `test >= cutoff`) để chống rò rỉ nhãn tương lai; tối ưu ngưỡng quyết định theo F1 trên tập validation; xử lý mất cân bằng lớp bằng `scale_pos_weight`.
- So sánh 3 mô hình trên cùng tập test trong `dwh.fact_early_warning_evaluation`: **Baseline luật tăng >= 25%**, **Logistic Regression** và **XGBoost** (`xgboost-cpu`, tự fallback sang `HistGradientBoostingClassifier` nếu thiếu thư viện).
- Xuất cảnh báo (`HIGH` / `WATCH` / `LOW`) kèm giải thích cục bộ 3 yếu tố chính **TreeSHAP** (`top_reasons_vi`) cho tập `test` và `forecast` (`H` tuần mới nhất) vào `dwh.fact_early_warning`, độ quan trọng đặc trưng vào `dwh.fact_early_warning_feature_importance` và lưu mô hình `data/models/early_warning_model.pkl`.

### 3.5. Khai phá Chuỗi chuyển dịch cụm Markov & Dwell Time (`covid_cluster_transitions`)
- Ước lượng ma trận xác suất chuyển cụm bậc 1 $P(C_{t+1} \mid C_t)$ vào `dwh.fact_cluster_transition` và thống kê độ dài các đợt lưu trú liên tục trong từng vùng dịch vào `dwh.fact_cluster_dwell_time`.

### 3.6. Khai phá Luật kết hợp trên Hồ sơ Bệnh nhân & Tiêm chủng (`patient_association_rules`)
- Kết hợp `dwh.dim_patient` và `dwh.fact_vaccination` để khai phá Top 40 quy luật `{Nhóm tuổi + Giới tính + Bệnh nền + Trạng thái tiêm chủng / Vắc-xin} ⇒ {Tình trạng COVID-19}` theo `Support`, `Confidence`, `Lift`, `Leverage`, `Conviction` vào `dwh.fact_patient_association_rules`.

### 3.7. Phát hiện Bất thường Kép (`covid_anomaly_detection` + Kafka Streaming) & Cổng AI (`:8501`)
- **Isolation Forest** quét 56.477 tuần – quốc gia phát hiện 1.130 tuần dị thường (`dwh.fact_covid_anomalies`).
- **StreamingOutbreakDetector** trong `cdc/consumer.py` giám sát cửa sổ trượt `300s` theo tỉnh/thành để bắn cảnh báo ổ dịch tức thời vào `dwh.fact_realtime_anomalies` (thử nghiệm trực tiếp bằng `.\scripts\demo_realtime_outbreak.ps1`).
- **Streamlit AI Portal (`http://localhost:8501`)**: 4 tab tiếng Việt cho phép mô phỏng kịch bản What-If trực tiếp trên mô hình XGBoost + biểu đồ TreeSHAP, tra cứu luật Apriori, dự phóng Markov $P^k$ và giám sát bất thường.

## 4. Biến môi trường cấu hình Data Mining

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `COVID_KMEANS_CLUSTERS` | `4` | Số cụm K cố định hoặc `auto` (chọn theo Silhouette lớn nhất) |
| `COVID_KMEANS_K_RANGE` | `2-8` | Khoảng giá trị K đưa vào bảng đánh giá Elbow / Silhouette |
| `COVID_EW_HORIZON_WEEKS` | `2` | Số tuần cảnh báo sớm trước khi bùng phát (`H`) |
| `COVID_EW_SURGE_RATIO` | `1.5` | Ngưỡng tỷ lệ tăng ca mắc sau `H` tuần (`>= 1.5` tức tăng ít nhất 50%) |
| `COVID_EW_MIN_CASES_PER_100K` | `10` | Mức ca tối thiểu trên 100.000 dân để tính là đợt bùng phát thực sự |
| `COVID_EW_TEST_FRACTION` | `0.2` | Tỷ lệ số tuần cuối dùng làm tập kiểm thử độc lập |
| `COVID_EW_INCLUDE_CLUSTER` | `true` | Đưa `cluster_id` của K-Means làm đặc trưng (đặt `false` khi chạy ablation) |

## 5. Khởi tạo tự động Datasets & Dashboards trên Apache Superset

Thay vì thêm từng view và dựng từng biểu đồ thủ công, chạy script khởi tạo tự động qua Superset REST API (`/api/v1/...`, idempotent — chạy nhiều lần không tạo bản trùng):

```powershell
python .\scripts\init_superset_dashboards.py
# hoặc qua PowerShell wrapper:
.\scripts\init_superset_dashboards.ps1
```

Script tự động đăng ký, đồng bộ cột và Việt hóa nhãn hiển thị (`verbose_name`) cho toàn bộ **17 Dataset Views** trong schema `dwh`, cập nhật **28 biểu đồ** và xuất bản **4 Dashboard tiếng Việt hoàn chỉnh**:

1. **Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu (4 Nhóm Xu hướng — 4 biểu đồ)**  
   `http://localhost:8089/superset/dashboard/covid19-kmeans-clusters/`
2. **Phân tích Phân cụm Dịch tễ COVID-19 — Đánh giá Số cụm K, PCA & Chuyển cụm Markov (8 biểu đồ)**  
   `http://localhost:8089/superset/dashboard/covid19-kmeans-mining/`
   - `vw_kmeans_evaluation` — biểu đồ **Độ phân tán nội cụm theo K (Phương pháp Khuỷu tay)** và **Độ tách biệt Cụm (Silhouette & Davies–Bouldin)**.
   - `vw_weekly_covid_clusters` — **Bản đồ Phân bố Cụm trên Không gian 2 Chiều (PCA)** và đường diễn biến ca mắc theo cụm.
   - `vw_kmeans_cluster_profile`, `vw_cluster_transition`, `vw_cluster_dwell_time` — hồ sơ cụm, **Ma trận xác suất chuyển dịch cụm Markov (%)** và **Bảng thời gian lưu trú liên tục (Dwell Time)**.
3. **Hệ thống Cảnh báo Sớm Bùng phát (XGBoost + TreeSHAP) & Bất thường (Isolation Forest) (7 biểu đồ)**  
   `http://localhost:8089/superset/dashboard/covid19-early-warning/`
   - `vw_early_warning_latest` — **Bản đồ Thế giới** xác suất bùng phát 2 tuần tới, biểu đồ tỷ lệ `Nguy cơ cao / Cần theo dõi / Nguy cơ thấp` và bảng xếp hạng quốc gia kèm giải thích TreeSHAP (`top_reasons_vi`).
   - `vw_early_warning_evaluation` & `vw_early_warning_feature_importance` — đối sánh 3 mô hình và Top 10 đặc trưng quan trọng nhất.
   - `vw_covid_anomalies` — phân loại và bảng chi tiết các tuần bất thường dịch tễ toàn cầu phát hiện bởi **Isolation Forest**.
4. **Giám sát Thời gian thực & Khai phá Luật Kết hợp — Bệnh nhân Ẩn danh & Tiêm chủng (9 biểu đồ)**  
   `http://localhost:8089/superset/dashboard/covid19-cdc-patients-vaccination/`
   - `vw_dashboard_patients`, `vw_dashboard_vaccinations`, `vw_vaccination_by_patient` — KPI thời gian thực, phân bố sức khỏe theo nhóm tuổi, cơ cấu bệnh nền, số lượt tiêm theo vắc-xin & độ tuổi và thống kê theo Tỉnh / Thành phố.
   - `vw_patient_association_rules` — **Top 10 Luật kết hợp dịch tễ có Độ nâng (Lift) cao nhất** và **Bảng khai phá luật kết hợp Apriori / FP-Growth**.
   - `vw_realtime_anomalies` — **Bảng cảnh báo ổ dịch bất thường thời gian thực (Kafka CDC)**.

## 6. Kiểm tra & Chạy Unit Test

```powershell
# Kiểm tra trạng thái CDC connector v2
curl.exe http://localhost:8083/connectors/vaccination-oltp-connector-v2/status

# Chạy unit test cho CDC consumer
python -m unittest discover -s cdc -p "test_*.py" -v

# Chạy unit test cho K-Means & Early Warning trong container Airflow
docker exec -i airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v

# Chạy kịch bản trình diễn kết quả Data Mining từ DWH
python .\scripts\demo_kmeans.py
```
