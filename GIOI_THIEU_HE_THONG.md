# Giới thiệu Tổng quan Hệ thống — Nền tảng Dữ liệu & Khai phá Dữ liệu Y tế Công cộng (COVID-19)

Tài liệu này trình bày tổng thể mục tiêu, kiến trúc kỹ thuật, mô hình dữ liệu kho (Data Warehouse), cơ chế đồng bộ thời gian thực (CDC) và hai bài toán **Khai phá dữ liệu (Data Mining)** cốt lõi của hệ thống, phục vụ trực tiếp cho việc tìm hiểu hệ thống, viết báo cáo và chuẩn bị slide bảo vệ.

*(Hướng dẫn cài đặt & cổng dịch vụ xem tại [README.md](./README.md); kịch bản thuyết trình trực tiếp xem tại [DEMO.md](./DEMO.md)).*

---

## 1. Bối cảnh & Mục tiêu Hệ thống

Trong giám sát dịch tễ học hiện đại, dữ liệu y tế thường phân mảnh ở nhiều định dạng và tốc độ phát sinh khác nhau: dữ liệu thống kê toàn cầu cập nhật theo lô (batch), danh mục chuẩn của Tổ chức Y tế Thế giới (WHO) khác hệ mã quốc gia, và dữ liệu tiêm chủng/ca bệnh nội bộ phát sinh liên tục theo thời gian thực kèm thông tin nhạy cảm của công dân.

Hệ thống được xây dựng nhằm giải quyết trọn vẹn bài toán từ **Kỹ thuật dữ liệu (Data Engineering)** đến **Khai phá dữ liệu (Data Mining)**:

1. **Hợp nhất đa nguồn dữ liệu thực tế** (OWID toàn cầu, WHO và MySQL OLTP nội bộ) qua kiến trúc kết hợp **Batch ETL + Real-time CDC**.
2. **Bảo vệ quyền riêng tư theo thiết kế (Privacy by Design)**: Tự động loại bỏ thông tin định danh cá nhân (`full_name`, `citizen_hash`) trước khi dữ liệu đi vào Kho dữ liệu phân tích.
3. **Phân cụm xu hướng dịch tễ không giám sát (Unsupervised Clustering — K-Means nâng cao)**: Tự động kiểm định số cụm tối ưu $K$, giải mã tâm cụm để sinh nhãn nghiệp vụ tiếng Việt và giảm chiều bằng **PCA** để trực quan hóa.
4. **Cảnh báo sớm bùng phát dịch có giám sát (Supervised Classification — XGBoost)**: Dự báo trước **2 tuần** nguy cơ bùng phát dịch tại từng quốc gia với cơ chế chia tập thời gian chống rò rỉ dữ liệu (**Purged Time-Series Split**).
5. **Trực quan hóa tự động 100% tiếng Việt trên Apache Superset**: Cung cấp 12 Dataset, 21 biểu đồ và 4 bảng điều khiển chuyên đề phục vụ ra quyết định.

---

## 2. Kiến trúc Tổng thể Hệ thống

Hệ thống vận hành hoàn toàn trên nền tảng container hóa (**Docker Compose** gồm 10 dịch vụ), chia làm 5 tầng rõ rệt:

```mermaid
flowchart LR
    subgraph Sources["1. Tầng Nguồn Dữ liệu"]
        OWID["OWID COVID-19 CSV<br/>(Toàn cầu - Mã ISO-3)"]
        WHO["WHO Daily CSV<br/>(15 Quốc gia - Mã ISO-2)"]
        OLTP[("MySQL 8.0 OLTP<br/>centers, patients, records")]
    end

    subgraph Ingestion["2. Tầng Thu thập & Đồng bộ"]
        AF["Apache Airflow 2.9<br/>(DAG: health_data_pipeline)"]
        DBZ["Debezium Connect 2.5<br/>(Đọc MySQL Binlog)"]
        KFK["Apache Kafka + ZooKeeper<br/>(3 Topics CDC)"]
        CDC["Python CDC Consumer<br/>(Ẩn danh PII & Upsert)"]
    end

    subgraph Storage["3. Tầng Lưu trữ (Lake & DWH)"]
        MINIO[("MinIO Data Lake<br/>(Bucket: covid19-landing-zone)")]
        DWH[("PostgreSQL 15 DWH<br/>(Schema: dwh - Snowflake)")]
    end

    subgraph Mining["4. Tầng Khai phá Dữ liệu"]
        KM["K-Means Clustering<br/>(Elbow, Silhouette, PCA 2D)"]
        XGB["Early Warning XGBoost<br/>(Dự báo trước H = 2 tuần)"]
    end

    subgraph BI["5. Tầng Trực quan hóa BI"]
        SUP["Apache Superset 3.1<br/>(4 Dashboard Tiếng Việt)"]
    end

    OWID --> AF
    WHO --> AF
    OLTP -. "Snapshot thô (bỏ full_name)" .-> AF
    AF --> MINIO
    MINIO --> AF
    AF --> DWH

    OLTP == "Binlog Row-level" ==> DBZ
    DBZ ==> KFK
    KFK ==> CDC
    CDC == "Upsert Idempotent" ==> DWH

    DWH --> KM
    KM -- "Ghi cụm, hồ sơ & PCA" --> DWH
    DWH --> XGB
    XGB -- "Ghi cảnh báo & đánh giá" --> DWH

    DWH --> SUP
```

### Vai trò từng thành phần công nghệ

| Tầng | Công nghệ | Vai trò chính trong dự án |
| :--- | :--- | :--- |
| **CSDL Giao dịch (OLTP)** | `MySQL 8.0` | Lưu trữ dữ liệu vận hành nội bộ (`vaccination_centers`, `patients`, `vaccination_records`), bật `binlog` chế độ `ROW`. |
| **Streaming CDC** | `Debezium` + `Kafka` + `Python` | Bắt biến động `INSERT/UPDATE/DELETE` theo thời gian thực (< 2 giây), ẩn danh bệnh nhân và đồng bộ sang DWH đảm bảo tính idempotent. |
| **Data Lake** | `MinIO` (chuẩn S3) | Lưu trữ dữ liệu thô (`Landing Zone`) phân vùng theo nguồn và ngày chạy (`raw/owid/`, `raw/who/`, `raw/oltp/`) phục vụ đối soát và chạy lại. |
| **Điều phối ETL & ML** | `Apache Airflow 2.9` | Tự động hóa chuỗi tác vụ từ tải dữ liệu, nạp DWH đến huấn luyện K-Means và XGBoost theo lịch `@daily`. |
| **Data Warehouse** | `PostgreSQL 15` | Lưu trữ kho dữ liệu phân tích theo lược đồ **Snowflake Schema** kết hợp các bảng lưu kết quả/chỉ số đánh giá mô hình học máy. |
| **Business Intelligence** | `Apache Superset 3.1` | Hiển thị 4 Dashboard tiếng Việt (21 biểu đồ), khởi tạo tự động qua REST API (`scripts/init_superset_dashboards.py`). |

---

## 3. Thiết kế Kho Dữ liệu (Snowflake Schema & Data Mining Marts)

Kho dữ liệu trong schema `dwh` được thiết kế tách bạch rõ phần lõi dịch tễ chuẩn hóa theo **Snowflake Schema** (`dim_country` nối phân cấp sang `dim_region`) và các bảng kết quả khai phá dữ liệu:

```mermaid
erDiagram
    DIM_REGION ||--o{ DIM_COUNTRY : "phân cấp châu lục"
    DIM_COUNTRY ||--o{ FACT_CASES : "country_id"
    DIM_DATE ||--o{ FACT_CASES : "date_id"
    DIM_INDICATOR ||--o{ FACT_CASES : "indicator_id"

    DIM_DATE ||--o{ DIM_PATIENT : "reported_date_id"
    DIM_VACCINATION_CENTER ||--o{ FACT_VACCINATION : "center_id"
    DIM_PATIENT ||--o{ FACT_VACCINATION : "patient_id"
    DIM_DATE ||--o{ FACT_VACCINATION : "date_id"

    DIM_REGION {
        int id PK
        varchar continent_code
        varchar continent_name
    }
    DIM_COUNTRY {
        int id PK
        varchar country_code "ISO-3"
        varchar country_name
        bigint population
        numeric gdp
        numeric beds_per_1000
        int region_id FK
    }
    DIM_DATE {
        int date_id PK "YYYYMMDD"
        date full_date
        smallint year
        smallint month
        boolean is_weekend
    }
    FACT_CASES {
        bigint id PK
        int country_id FK
        int date_id FK
        int new_cases
        int new_deaths
        bigint people_vaccinated
        bigint people_fully_vaccinated
    }
    DIM_PATIENT {
        bigint patient_id PK
        smallint birth_year
        char gender
        varchar province
        varchar health_status
        varchar comorbidity
        boolean is_placeholder
    }
    FACT_VACCINATION {
        bigint record_id PK
        int center_id FK
        bigint patient_id FK
        int date_id FK
        varchar vaccine_type
        smallint dose_number
    }
```

### Các bảng lưu trữ kết quả Data Mining trong `dwh`

1. **Nhóm bảng Phân cụm K-Means**:
   - `dwh.fact_weekly_covid_cluster`: Kết quả gán cụm của **56.477** quan sát `(week_start, country_code)`, kèm `cluster_label` và tọa độ giảm chiều `pca_1`, `pca_2`.
   - `dwh.fact_kmeans_evaluation`: Bảng chỉ số kiểm định `K = 2..8` (`inertia`, `silhouette`, `davies_bouldin`, `calinski_harabasz`, `is_selected`, `is_best_silhouette`).
   - `dwh.fact_kmeans_cluster_profile`: Hồ sơ thống kê từng cụm (số lượng, tỉ trọng, trung bình/trung vị ca mắc & tử vong, tâm cụm không gian log gốc, Silhouette trung bình từng cụm).
2. **Nhóm bảng Cảnh báo sớm XGBoost**:
   - `dwh.fact_early_warning`: Xác suất bùng phát `surge_probability`, ngưỡng quyết định, nhãn dự báo và mức cảnh báo (`HIGH / WATCH / LOW`) cho cả tập `test` (backtest có nhãn thật) và `forecast` (2 tuần mới nhất).
   - `dwh.fact_early_warning_evaluation`: Bảng đối sánh định lượng 3 mô hình trên cùng tập kiểm thử (`accuracy`, `precision`, `recall`, `f1_score`, `roc_auc`, `pr_auc`, ma trận nhầm lẫn).
   - `dwh.fact_early_warning_feature_importance`: Bảng xếp hạng mức độ đóng góp của 18 đặc trưng đầu vào.

### Nguyên tắc Tích hợp Dữ liệu & Bảo mật Đáng chú ý
- **Không trộn sai hệ mã quốc gia**: Dữ liệu OWID dùng mã ISO-3 (`VNM`, `USA`) được nạp vào `dwh.fact_cases`, trong khi bộ WHO dùng mã ISO-2 (`VN`, `US`) được lưu ở bảng độc lập `dwh.fact_who_covid_daily`.
- **Ẩn danh hóa dữ liệu bệnh nhân (PII Anonymization)**: Bảng `patients` ở MySQL chứa `full_name` và `citizen_hash`, nhưng `cdc/consumer.py` cắt bỏ hoàn toàn 2 trường này trước khi ghi sang `dwh.dim_patient`.
- **Xử lý sự kiện lệch thứ tự trên Kafka**: Nếu bản ghi tiêm chủng tới trước bản ghi bệnh nhân, consumer tự tạo dòng tạm `dim_patient(is_placeholder = TRUE)` để bảo toàn khóa ngoại, sau đó tự cập nhật đầy đủ khi bản tin bệnh nhân tới.

---

## 4. Chi tiết Hai Bài toán Khai phá Dữ liệu (Data Mining)

### 4.1. Bài toán 1: Phân cụm Xu hướng Dịch tễ K-Means Nâng cao
*(Mã nguồn: `airflow/dags/weekly_covid_kmeans.py`)*

- **Đơn vị quan sát**: Quốc gia $c$ trong tuần dịch tễ $t$ (tổng cộng **56.477** quan sát thực tế từ năm 2020–2024).
- **Không gian 3 đặc trưng dịch tễ** (đã chuẩn hóa theo quy mô dân số và biến đổi logarit để giảm độ lệch phải của các đợt siêu bùng phát, sau đó chuẩn hóa Z-score bằng `StandardScaler`):
  $$X_1 = \ln\left(1 + \frac{\text{cases}_{c,t}}{\text{pop}_c} \times 10^5\right), \quad X_2 = \ln\left(1 + \frac{\text{deaths}_{c,t}}{\text{pop}_c} \times 10^5\right), \quad X_3 = X_{1(c,t)} - X_{1(c,t-1)}$$
- **Kiểm định số cụm $K \in \{2, \dots, 8\}$**: Tính đồng thời 4 thước đo độc lập (với cỡ mẫu ngẫu nhiên cố định 10.000 dòng cho Silhouette để tối ưu độ phức tạp $O(n^2)$). Hệ thống hỗ trợ cả chọn cố định `K=4` lẫn chế độ tự động `COVID_KMEANS_CLUSTERS=auto` (`K=5` đạt Silhouette cực đại `0,5821`).
- **Hồ sơ cụm & Tự động gắn nhãn**: Các cụm được sắp thứ tự tăng dần theo tỷ lệ mắc trung bình; tâm cụm được biến đổi ngược (`scaler.inverse_transform`) để nhận diện xu hướng (`đang tăng` khi $\Delta \ln > 0.15$, `đang giảm` khi $\Delta \ln < -0.15$, ngược lại là `ổn định`).

| Cụm | Nhãn tự sinh | Số quan sát | Tỉ trọng | Ca mắc TB / 100k | Tử vong TB / 100k | Ý nghĩa dịch tễ thực tế |
| :---: | :--- | ---: | ---: | ---: | ---: | :--- |
| **0** | `Thấp – ổn định` | 35.236 | 62,4% | 0,9 | 0,02 | Kiểm soát tốt / Vùng xanh bình thường |
| **1** | `Trung bình – ổn định` | 14.900 | 26,4% | 136,4 | 0,36 | Lây nhiễm cộng đồng trong tầm kiểm soát |
| **2** | `Cao – đang tăng` | 770 | 1,4% | 332,1 | 0,39 | Đợt bùng phát tăng vọt đột biến (+705%/tuần) |
| **3** | `Rất cao – ổn định` | 5.571 | 9,9% | 442,4 | 4,14 | Đỉnh dịch kéo dài / Áp lực tử vong cao |

---

### 4.2. Bài toán 2: Cảnh báo Sớm Bùng phát Dịch bằng XGBoost
*(Mã nguồn: `airflow/dags/covid_early_warning.py`)*

- **Định nghĩa bài toán phân lớp nhị phân**: Tại tuần $t$ của quốc gia $c$, dự đoán nhãn bùng phát sau $H = 2$ tuần:
  $$y_{c,t} = \mathbb{I}\Big(\text{rate}_{c, t+H} \ge 1.5 \times \text{rate}_{c,t} \;\wedge\; \text{rate}_{c, t+H} \ge 10\Big)$$
- **18 đặc trưng nhân quả nghiêm ngặt** (chỉ sử dụng thông tin $\le t$, tuyệt đối không nhìn tương lai):
  - *Mức độ & Trễ*: `log_cases_per_100k`, trễ 1–3 tuần, trung bình & độ lệch chuẩn trượt 4 tuần.
  - *Động lượng*: tốc độ tăng 1 tuần, 2 tuần và **gia tốc lây nhiễm** (`growth_accel`).
  - *Tử vong*: `log_deaths_per_100k`, tăng trưởng tử vong 1 tuần, tỷ lệ tử vong/ca mắc trượt 4 tuần (`cfr_roll4`).
  - *Miễn dịch & Bối cảnh*: tỷ lệ tiêm $\ge 1$ mũi và tiêm đủ (`cummax / population`), chu kỳ mùa vụ (`week_sin`, `week_cos`), xu hướng tăng trung bình toàn cầu cùng tuần và `cluster_id` từ K-Means.
- **Chia tập thời gian có khoảng trống chống rò rỉ (Purged Time Split)**:
  - Tập huấn luyện chỉ lấy các tuần $t < \text{cutoff} - H$; tập kiểm thử lấy $t \ge \text{cutoff}$; $H$ tuần mới nhất của mỗi quốc gia đưa vào tập dự báo thực tế (`forecast`).
  - Tối ưu ngưỡng báo động theo **F1-Score** trên tập validation (15% tuần cuối của tập train) và cân bằng lớp hiếm bằng `scale_pos_weight`.
- **Kết quả thực nghiệm độc lập** (44.520 mẫu huấn luyện / 11.006 mẫu kiểm thử):

| Mô hình | Ngưỡng tối ưu | Precision | Recall | F1-Score | ROC-AUC | PR-AUC (Lớp hiếm) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **XGBoost (Mô hình chính)** | **0,64** | **0,244** | 0,663 | **0,356** | **0,925** | **0,270** |
| Hồi quy Logistic (`balanced`) | 0,40 | 0,173 | **0,750** | 0,282 | 0,876 | 0,165 |
| Luật cơ sở (Tăng tuần trước $\ge 25\%$) | 0,50 | 0,104 | 0,347 | 0,161 | 0,728 | 0,070 |

*(Kiểm chứng Ablation khi loại bỏ `cluster_id` khỏi XGBoost cho kết quả `ROC-AUC = 0,926`, `PR-AUC = 0,264`, `F1 = 0,355` — khẳng định mô hình hoàn toàn trung thực, không bị rò rỉ thông tin từ tâm cụm).*

---

## 5. Hệ thống 4 Bảng Điều khiển (Dashboard) Tiếng Việt trên Superset

Toàn bộ giao diện phân tích tại `http://localhost:8089` được khởi tạo tự động qua REST API (`scripts/init_superset_dashboards.py`):

1. **Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu (4 Nhóm Xu hướng)** (`/superset/dashboard/covid19-kmeans-clusters/`): Góc nhìn tổng thể mối quan hệ ca mắc – tử vong – quy mô dân số giữa 4 cụm.
2. **Phân tích Phân cụm Dịch tễ COVID-19 — Đánh giá Số cụm K & Hồ sơ Cụm** (`/superset/dashboard/covid19-kmeans-mining/`): Biểu đồ Khuỷu tay, Silhouette/Davies–Bouldin, bản đồ phân cụm 2 chiều PCA và bảng hồ sơ cụm.
3. **Hệ thống Cảnh báo Sớm Nguy cơ Bùng phát Dịch COVID-19** (`/superset/dashboard/covid19-early-warning/`): Bản đồ thế giới cảnh báo sớm 2 tuần tới, đối sánh 3 mô hình, Top 10 yếu tố ảnh hưởng và bảng xếp hạng quốc gia nguy cơ cao.
4. **Giám sát Thời gian thực — Hồ sơ Bệnh nhân Ẩn danh & Tiêm chủng Nội bộ** (`/superset/dashboard/covid19-cdc-patients-vaccination/`): Thẻ KPI thời gian thực, phân tích ca bệnh theo độ tuổi, bệnh nền, loại vắc-xin và tỉnh/thành phố.

---

## 6. Cấu trúc Thư mục Mã nguồn Chính

```text
BTL-covid19/
├── airflow/
│   ├── dags/
│   │   ├── health_data_pipeline.py       # DAG điều phối ETL + K-Means + Early Warning
│   │   ├── weekly_covid_kmeans.py        # Thuật toán K-Means, đánh giá K, hồ sơ cụm & PCA
│   │   └── covid_early_warning.py        # Mô hình cảnh báo sớm XGBoost & Purged Time Split
│   └── tests/                            # 13 Unit tests kiểm định ML & chống rò rỉ dữ liệu
├── cdc/
│   ├── consumer.py                       # Kafka CDC consumer (ẩn danh bệnh nhân & upsert DWH)
│   └── test_consumer.py                  # 10 Unit tests kiểm định CDC & idempotency
├── config/
│   ├── debezium/                         # Cấu hình connector MySQL v2
│   ├── mysql/                            # Cấu hình binlog MySQL OLTP
│   └── superset/                         # Cấu hình bảo mật & tính năng Apache Superset
├── scripts/
│   ├── sql/
│   │   ├── init-oltp.sql                 # Khởi tạo schema & dữ liệu gốc MySQL OLTP
│   │   ├── init-dwh.sql                  # Khởi tạo Snowflake Schema trên PostgreSQL DWH
│   │   └── upgrade-dwh.sql               # Migration idempotent cho CDC, K-Means, XGBoost & Views tiếng Việt
│   ├── seed_oltp_data.py                 # Sinh hàng nghìn bệnh nhân & mũi tiêm giả lập cho OLTP
│   ├── init_superset_dashboards.py       # Tự động tạo & Việt hóa 12 Dataset, 21 Chart, 4 Dashboard
│   ├── demo_cdc.ps1 / demo_cdc_stream.py # Kịch bản minh họa đồng bộ CDC thời gian thực
│   └── demo_kmeans.py                    # Kịch bản truy vấn & trình diễn kết quả Data Mining trên CLI
├── docker-compose.yml                    # Định nghĩa 10 dịch vụ hạ tầng toàn hệ thống
├── GIOI_THIEU_HE_THONG.md                # Tài liệu giới thiệu tổng quan kiến trúc & thuật toán (file này)
├── DEMO.md                               # Kịch bản thuyết trình & hỏi đáp bảo vệ đồ án
└── README.md                             # Hướng dẫn cài đặt, ánh xạ cổng & vận hành nhanh
```
