# Kế hoạch nâng cấp Data Mining — BTL COVID-19

Mục tiêu gồm 3 hạng mục, làm theo đúng khuôn đã có của project:
**Airflow task → bảng `dwh.fact_*` → view `dwh.vw_*` → dashboard Superset**.

| # | Hạng mục | Kỹ thuật Data Mining | Trạng thái |
|---|----------|----------------------|------------|
| 1 | Cập nhật CDC: thêm bảng `patients` | Tích hợp dữ liệu, ẩn danh hóa | Hoàn thành (đã chạy E2E trên Docker + seed 1.500+ bệnh nhân) |
| 2 | K-means: đánh giá và mô tả cụm | Clustering, Elbow, Silhouette, PCA | Hoàn thành (code + unit test + chạy trên 56.477 dòng thật) |
| 3 | Cảnh báo sớm bùng phát dịch | Classification (XGBoost), đánh giá mô hình | Hoàn thành (code + unit test + backtest/ablation trên DWH) |

---

## 1. Cập nhật CDC — đồng bộ thêm bảng `patients`

### Vấn đề hiện tại
- Debezium mới capture 2 bảng `vaccination_centers` và `vaccination_records`. Bảng `patients` (năm sinh, giới tính, tỉnh, tình trạng, bệnh nền) chưa vào DWH.
- Vì vậy chưa phân tích được tiêm chủng theo nhóm tuổi, giới tính hay bệnh nền.

### Thiết kế
- **Bảng mới `dwh.dim_patient`**: chỉ giữ thuộc tính phân tích. **Không** lưu `full_name` và `citizen_hash` (tối thiểu hóa dữ liệu cá nhân).
- **`dwh.fact_vaccination`** thêm cột `patient_id`, khóa ngoại tới `dim_patient` với `ON DELETE SET NULL`.
- **Bản ghi placeholder**: event tiêm chủng có thể tới trước event bệnh nhân vì chúng nằm ở hai topic Kafka khác nhau. Khi đó consumer tạo trước một dòng `dim_patient` với `is_placeholder = TRUE`, giống cách đang làm với trung tâm tiêm.
- **Connector mới `vaccination-oltp-connector-v2`**:
  - Debezium **không snapshot lại** bảng mới thêm vào `table.include.list` của connector đã có offset, nên bảng `patients` sẽ thiếu dữ liệu cũ.
  - Cách xử lý: tạo connector mới (`server.id` mới, topic schema history mới). Connector này snapshot lại toàn bộ, consumer upsert idempotent nên không bị trùng.
  - Consumer tự xóa connector cũ (`DEBEZIUM_LEGACY_CONNECTORS`) để tránh nhận event hai lần.
- **View mới**:
  - `vw_dashboard_patients`: số bệnh nhân theo tỉnh, giới tính, nhóm tuổi, tình trạng, bệnh nền.
  - `vw_vaccination_by_patient`: số mũi tiêm theo vaccine, mũi, nhóm tuổi, giới tính, bệnh nền.

### File thay đổi
| File | Nội dung |
|------|----------|
| `cdc/consumer.py` | `apply_patient_event`, liên kết `patient_id` trong `apply_record_event`, `remove_legacy_connectors`, regex topic có `patients`, xử lý truncate |
| `cdc/test_consumer.py` | 4 test mới: ẩn danh hóa, xóa theo key, placeholder, gỡ connector cũ |
| `config/debezium/register-mysql-connector.json` | Tên v2, `server.id` 184055, thêm `patients` |
| `docker-compose.yml` | `DEBEZIUM_CONNECTOR_NAME`, `DEBEZIUM_LEGACY_CONNECTORS` |
| `scripts/sql/upgrade-dwh.sql` | `dim_patient`, cột `patient_id`, 2 view |
| `README.md`, `DEMO.md` | Đổi URL status connector sang v2 |

### Việc còn lại
- [x] (Tùy chọn) Task `extract_mysql_oltp` xuất thêm `patients` lên MinIO, bỏ cột `full_name`.
- [x] (Tùy chọn) Mở rộng `scripts/demo_cdc_stream.py` (và `scripts/demo_cdc.ps1`) để insert bệnh nhân mới rồi kiểm tra trong `dim_patient`.
- [x] Viết script `scripts/seed_oltp_data.py` sinh hàng nghìn bệnh nhân/mũi tiêm giả lập phục vụ dashboard.

---

## 2. K-means — đánh giá và mô tả cụm

### Vấn đề hiện tại
- `K = 4` đang đặt cứng qua env `COVID_KMEANS_CLUSTERS` và chưa có căn cứ.
- Chưa có chỉ số chất lượng cụm. Các cụm chỉ có số ID, không có ý nghĩa nghiệp vụ.

### Thiết kế
Giữ nguyên 3 feature hiện có: `log1p(cases/100k)`, `log1p(deaths/100k)`, `log` biến động so với tuần trước, chuẩn hóa bằng `StandardScaler`.

**a) Chọn K có căn cứ.** Chạy K từ 2 đến 8 (env `COVID_KMEANS_K_RANGE=2-8`) và tính:

| Chỉ số | Ý nghĩa | Tốt khi |
|--------|---------|---------|
| Inertia (WCSS) | Vẽ biểu đồ Elbow | Tìm “khuỷu tay” |
| Silhouette | Độ tách biệt giữa các cụm | Cao (gần 1) |
| Davies–Bouldin | Tỉ lệ độ phân tán / khoảng cách cụm | Thấp |
| Calinski–Harabasz | Tỉ lệ phương sai giữa / trong cụm | Cao |

- Silhouette tốn O(n²), nên tính trên mẫu 10.000 dòng (`sample_size`, `random_state=42`). Dữ liệu có khoảng 56 nghìn dòng.
- Cho phép `COVID_KMEANS_CLUSTERS=auto` để chọn K có Silhouette cao nhất. Mặc định vẫn là 4 để dashboard ổn định.
- Lưu vào **`dwh.fact_kmeans_evaluation`**, đánh dấu `is_selected` (K đang dùng) và `is_best_silhouette`.

**b) Hồ sơ cụm (cluster profiling)** lưu trong **`dwh.fact_kmeans_cluster_profile`**:
- Số phần tử và tỉ trọng; trung bình/trung vị ca mắc trên 100k dân; tử vong trên 100k dân; trung vị % tăng trưởng.
- Tâm cụm ở không gian log gốc (`scaler.inverse_transform`).
- Silhouette trung bình riêng từng cụm (`silhouette_samples` trên mẫu).
- **Nhãn cụm tự sinh**: mức độ theo thứ hạng ca mắc (Thấp / Trung bình / Cao / Rất cao) cộng xu hướng theo tâm `log_change` (`> 0.15` là đang tăng, `< -0.15` là đang giảm, còn lại là ổn định). Ví dụ: *“Cao – đang tăng”*.

**c) Trực quan hóa**: PCA 2 thành phần, lưu thành cột `pca_1`, `pca_2` và `cluster_label` trong `fact_weekly_covid_cluster`. Superset dùng để vẽ scatter cụm.

### Cấu trúc code (`airflow/dags/weekly_covid_kmeans.py`)
```text
prepare_weekly_features(records)          -> (enriched_records, X_scaled, scaler)
evaluate_cluster_counts(X, k_values)      -> list[dict]  (inertia, silhouette, DB, CH)
select_cluster_count(evaluations, req)    -> int         (số cố định hoặc "auto")
build_cluster_profiles(records, labels..) -> list[dict]
run_weekly_clustering(records, ...)       -> {records, evaluations, profiles, cluster_count}
cluster_weekly_records(records, k)        -> giữ nguyên chữ ký cũ (tương thích ngược, test cũ vẫn chạy)
```
`cluster_weekly_covid_callable` trong `health_data_pipeline.py` ghi thêm 2 bảng mới và 3 cột mới. `model_version` giữ là `weekly-kmeans-v1` vì feature không đổi, tránh dữ liệu trùng trên dashboard cũ.

### Test (`airflow/tests/test_weekly_covid_kmeans.py`)
- [x] Bảng đánh giá có đủ các K trong khoảng, và đúng một dòng `is_selected`.
- [x] `auto` chọn đúng K có Silhouette lớn nhất.
- [x] Bỏ qua K lớn hơn số điểm khác nhau, không bị crash.
- [x] Nhãn cụm sắp đúng thứ tự mức ca mắc.
- [x] Các test cũ vẫn pass.

---

## 3. Cảnh báo sớm bùng phát dịch bằng XGBoost

### Bài toán
Với mỗi quốc gia tại tuần *t*, dự đoán: **sau H tuần, ca mắc có bùng phát không?** Đây là bài toán phân lớp nhị phân.

**Định nghĩa nhãn** (cấu hình qua env):
```text
surge = 1  nếu  rate(t+H) >= SURGE_RATIO × rate(t)   và   rate(t+H) >= MIN_RATE
```
| Env | Mặc định | Ý nghĩa |
|-----|----------|---------|
| `COVID_EW_HORIZON_WEEKS` | 2 | Cảnh báo trước 2 tuần |
| `COVID_EW_SURGE_RATIO` | 1.5 | Tăng ít nhất 50% |
| `COVID_EW_MIN_CASES_PER_100K` | 10 | Bỏ qua nhiễu khi số ca quá thấp |
| `COVID_EW_TEST_FRACTION` | 0.2 | 20% số tuần cuối dùng làm tập test |

### Feature (chỉ dùng thông tin đến tuần *t*, không nhìn tương lai)
| Nhóm | Feature |
|------|---------|
| Mức ca mắc | `log_rate`, lag 1–3 tuần, trung bình và độ lệch chuẩn trượt 4 tuần |
| Động lượng | tăng trưởng 1 tuần, tăng trưởng 2 tuần, gia tốc (thay đổi của tốc độ tăng) |
| Tử vong | `log_death_rate`, tăng trưởng tử vong, CFR trượt 4 tuần |
| Tiêm chủng | Tỉ lệ dân đã tiêm ≥1 mũi và tiêm đủ (`cummax` / dân số, từ OWID) |
| Mùa vụ | `sin`/`cos` của tuần trong năm |
| Bối cảnh toàn cầu | Tăng trưởng trung bình của mọi quốc gia trong cùng tuần |
| Kết quả K-means | `cluster_id` (đã sắp theo mức độ nghiêm trọng), nối mục 2 với mục 3 |

### Chia dữ liệu và đánh giá
- **Chia theo thời gian, có purge**: train dùng các tuần `< cutoff − H`, test dùng các tuần `≥ cutoff`. Khoảng trống H tuần ngăn nhãn của train “nhìn” sang giai đoạn test.
- **Chọn ngưỡng quyết định** trên tập validation (15% số tuần cuối của train) theo F1 lớn nhất. Sau đó train lại trên toàn bộ train rồi đánh giá trên test.
- **Xử lý mất cân bằng lớp**: `scale_pos_weight = số âm / số dương`.
- **So sánh 3 mô hình** trên cùng tập test:
  1. *Baseline luật*: cảnh báo khi tuần trước tăng ≥ 25%.
  2. *Logistic Regression* (`class_weight="balanced"`).
  3. **XGBoost** (`n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8`).
- **Chỉ số**: Accuracy, Precision, Recall, F1, **ROC-AUC**, **PR-AUC** (quan trọng vì lớp dương hiếm), confusion matrix.
- **Giải thích mô hình**: feature importance (gain) của XGBoost. Có thể thêm SHAP nếu còn thời gian.

### Đầu ra
| Bảng | Nội dung |
|------|----------|
| `fact_early_warning` | Xác suất bùng phát theo quốc gia-tuần. `split='test'` là backtest có nhãn thật; `split='forecast'` là H tuần mới nhất chưa có nhãn |
| `fact_early_warning_evaluation` | Chỉ số của 3 mô hình, ngưỡng, khoảng thời gian test |
| `fact_early_warning_feature_importance` | Độ quan trọng của feature |

**Mức cảnh báo** (dựa trên ngưỡng *t* đã chọn): `HIGH` khi p ≥ t; `WATCH` khi t/2 ≤ p < t; còn lại là `LOW`.
View `vw_early_warning_latest` lấy cảnh báo mới nhất của từng quốc gia cho dashboard.

### Cấu trúc code
- File mới `airflow/dags/covid_early_warning.py` chỉ chứa hàm thuần, test được không cần DB:
  `build_feature_frame`, `purged_time_split`, `fit_with_threshold`, `evaluate_models`, `predict_alerts`.
- Thêm `early_warning_callable` vào `health_data_pipeline.py`. Hàm này đọc `fact_weekly_covid_cluster` join với độ phủ vaccine tuần từ `fact_cases`.
- DAG: `transform_and_load_to_dwh >> weekly_covid_kmeans >> covid_early_warning`.
- **Dependency**: `xgboost-cpu==2.1.4` (wheel 4.5 MB). Bản `xgboost` thường kéo theo CUDA/NCCL khoảng 200 MB và sẽ bị cài lại mỗi lần container khởi động. Thêm vào `_PIP_ADDITIONAL_REQUIREMENTS` trong `docker-compose.yml` và `airflow/requirements.txt`.
- **Dự phòng**: nếu import xgboost lỗi thì dùng `HistGradientBoostingClassifier` của sklearn. Cột `model_name` ghi lại mô hình thực sự đã dùng.

### Test (`airflow/tests/test_covid_early_warning.py`)
- [x] Nhãn đúng với dữ liệu tổng hợp: chuỗi tăng gấp đôi có nhãn 1, chuỗi phẳng có nhãn 0.
- [x] Feature lag không lấy dữ liệu tương lai: đổi giá trị ở tuần t+1 thì feature tại t không đổi.
- [x] Purge: tuần lớn nhất của train nhỏ hơn `cutoff − H`.
- [x] H tuần cuối mỗi quốc gia rơi vào `forecast`, không có nhãn.
- [x] Chạy được khi chỉ có một lớp: AUC trả `None`, không crash.

---

## 4. Thứ tự thực hiện

1. **Migration DWH** (`upgrade-dwh.sql`): đã viết xong, cần chạy thử `dwh-migrate` hai lần để kiểm tra tính idempotent.
2. **CDC**: đã xong code và test. Cần kiểm tra trên Docker: connector v2 ở trạng thái RUNNING, connector cũ đã bị xóa, `dim_patient` có 6 dòng.
3. **K-means**: refactor module, thêm test, rồi cập nhật callable.
4. **Early warning**: viết module, test, callable, thêm dependency và nối vào DAG.
5. **Superset** (`scripts/init_superset_dashboards.py` — tự động hóa qua REST API): đã đăng ký 12 dataset view, 17 chart và 3 dashboard:
   - Elbow (inertia theo K) và Silhouette theo K (`covid19-kmeans-mining`)
   - Scatter/Bubble PCA tô màu theo cụm (`covid19-kmeans-mining`)
   - Bảng hồ sơ cụm (`covid19-kmeans-mining`)
   - Bản đồ / bảng cảnh báo mới nhất (`covid19-early-warning`)
   - So sánh ROC-AUC / PR-AUC của 3 mô hình (`covid19-early-warning`)
   - Biểu đồ feature importance (`covid19-early-warning`)
   - Phân tích bệnh nhân ẩn danh & tiêm chủng CDC (`covid19-cdc-patients-vaccination`)
6. **Tài liệu**: cập nhật `README.md` (bảng/view/env mới) và `DEMO.md` (kịch bản demo), sửa `scripts/demo_kmeans.py` để in thêm bảng đánh giá.

## 5. Kiểm tra end-to-end

```powershell
docker compose up -d --build
docker compose up -d --force-recreate dwh-migrate cdc-consumer airflow-webserver airflow-scheduler
curl.exe http://localhost:8083/connectors/vaccination-oltp-connector-v2/status
docker exec -i airflow-scheduler airflow tasks test health_data_pipeline weekly_covid_kmeans 2026-11-26
docker exec -i airflow-scheduler airflow tasks test health_data_pipeline covid_early_warning 2026-11-26
docker exec -i postgres-dwh psql -U dwh_admin -d health_dwh -c "SELECT * FROM dwh.vw_kmeans_evaluation ORDER BY k;"
docker exec -i postgres-dwh psql -U dwh_admin -d health_dwh -c "SELECT model_name, f1_score, roc_auc, pr_auc FROM dwh.vw_early_warning_evaluation;"
```

Test đơn vị:
```powershell
cd cdc; python -m unittest test_consumer -v
docker exec -i airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v
```
Lưu ý: trên máy Windows này, scikit-learn/pandas bị chính sách Application Control chặn, nên test ML phải chạy trong container Airflow. Muốn vậy cần mount thêm `./airflow/tests` vào container.

## 6. Rủi ro và lưu ý khi viết báo cáo

- **Dữ liệu OWID sau 2023** chuyển sang báo cáo theo tuần, nhiều nước ngừng báo cáo. Tập test (giai đoạn cuối) có ít ca bùng phát, nên PR-AUC có thể thấp. Cần nêu rõ trong báo cáo và có thể thử thêm `COVID_EW_TEST_FRACTION`.
- **Rò rỉ nhẹ từ `cluster_id`**: K-means fit trên toàn bộ giai đoạn. Feature chỉ dùng dữ liệu của cùng tuần, nhưng tâm cụm có “thấy” dữ liệu tương lai. Nên chạy thêm một lần không có `cluster_id` để so sánh (ablation).
- **Tuần cuối cùng có thể chưa đủ 7 ngày**, khiến số ca thấp giả tạo. Nên loại tuần chưa đủ ngày khỏi `forecast`.
- Đây là **cảnh báo xu hướng phục vụ học thuật**, không phải hệ thống cảnh báo y tế chính thức.
