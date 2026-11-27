# Kịch bản Demo Bảo vệ Đồ án (Data Platform & Data Mining)

Thời lượng gợi ý: **10 – 12 phút**.  
Luồng trình bày xuyên suốt: **Thu thập đa nguồn (OWID/WHO/MySQL) → Data Lake (MinIO) → Kho dữ liệu Snowflake (PostgreSQL) → 5 Trụ cột Data Mining (K-Means/PCA, Chuỗi Markov, Cảnh báo sớm XGBoost + TreeSHAP, Luật kết hợp Apriori, Isolation Forest & Ổ dịch Kafka Realtime) → 4 Dashboard Superset (28 biểu đồ) & Cổng Web AI Tương tác Streamlit (`:8501`)**.

---

## 0. Checklist chuẩn bị 5 phút trước giờ bảo vệ

1. **Đảm bảo 11 container đang chạy ổn định:**
   ```powershell
   docker compose ps
   ```
   *(Nếu vừa bật máy lại: chạy `docker compose up -d`)*

2. **Đồng bộ 4 Dashboard tiếng Việt (17 Dataset Views, 28 biểu đồ) trên Superset (nếu cần làm mới):**
   ```powershell
   python .\scripts\init_superset_dashboards.py
   ```

3. **Mở sẵn các tab trình duyệt (tài khoản mặc định đều là `admin` / `admin`):**
   - **Tab 1 — Airflow UI**: `http://localhost:8081` (mở sẵn Graph của DAG `health_data_pipeline`)
   - **Tab 2 — MinIO Console**: `http://localhost:9001` (`minioadmin` / `minioadmin123`, mở bucket `covid19-landing-zone`)
   - **Tab 3 — Dashboard 1 (Tổng quan K-Means)**: `http://localhost:8089/superset/dashboard/covid19-kmeans-clusters/`
   - **Tab 4 — Dashboard 2 (Đánh giá K & PCA)**: `http://localhost:8089/superset/dashboard/covid19-kmeans-mining/`
   - **Tab 5 — Dashboard 3 (Cảnh báo sớm XGBoost)**: `http://localhost:8089/superset/dashboard/covid19-early-warning/`
   - **Tab 6 — Dashboard 4 (CDC Bệnh nhân, Tiêm chủng & Luật Kết hợp Apriori)**: `http://localhost:8089/superset/dashboard/covid19-cdc-patients-vaccination/`
   - **Tab 7 — Cổng AI Tương tác (Streamlit Portal)**: `http://localhost:8501` (Mô phỏng What-If XGBoost + TreeSHAP, Tra cứu Apriori, Markov $P^k$ & Cảnh báo ổ dịch Realtime)
   - **Cửa sổ PowerShell**: đứng sẵn tại thư mục gốc `BTL-covid19` để chạy lệnh live (`demo_cdc.ps1`, `demo_realtime_outbreak.ps1`).

---

## 1. Mở đầu & Tổng quan Kiến trúc | 1 phút

**Thao tác:** Chiếu `docker compose ps` hoặc sơ đồ luồng dữ liệu.

**Lời dẫn gợi ý:**
> "Đề tài xây dựng nền tảng tích hợp và khai phá dữ liệu y tế công cộng kết hợp hai luồng:  
> (1) **Batch ETL** qua Apache Airflow thu thập dữ liệu toàn cầu từ OWID và WHO, lưu thô phân vùng theo ngày trên MinIO Data Lake rồi chuẩn hóa vào PostgreSQL theo mô hình Snowflake Schema;  
> (2) **Streaming CDC thời gian thực** qua Debezium và Kafka đồng bộ hồ sơ bệnh nhân và lịch sử tiêm chủng từ MySQL OLTP sang DWH có ẩn danh hóa dữ liệu cá nhân, đồng thời giám sát cửa sổ trượt phát hiện ổ dịch tức thời.  
> Trên nền tảng 11 container đó, nhóm triển khai trọn bộ **5 bài toán Khai phá dữ liệu bổ trợ lẫn nhau**: Phân cụm K-Means nâng cao + PCA 2D, Chuỗi chuyển dịch cụm Markov & Dwell Time, Cảnh báo sớm bùng phát trước 2 tuần bằng **XGBoost + TreeSHAP**, Khai phá Luật kết hợp **Apriori** trên bệnh nhân CDC và Phát hiện bất thường kép (**Isolation Forest + Kafka Streaming**), hiển thị trên **4 Dashboard Superset (28 biểu đồ)** cùng **Cổng Web AI Tương tác Streamlit (`:8501`)**."

---

## 2. Luồng Điều phối Airflow & Data Lake MinIO | 1.5 phút

**Thao tác:**
1. Chuyển sang **Tab 1 (Airflow)** → mở tab **Graph** của DAG `health_data_pipeline`, chỉ vào chuỗi phụ thuộc tự động từ ETL sang 5 mô hình Data Mining:  
   `[extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh >> weekly_covid_kmeans >> [covid_cluster_transitions, covid_early_warning, covid_anomaly_detection]` và nhánh `patient_association_rules`.
2. Chuyển sang **Tab 2 (MinIO)** → mở `raw/owid/`, `raw/who/`, `raw/oltp/`.

**Lời dẫn gợi ý:**
> "Toàn bộ quy trình từ trích xuất, tiền xử lý đến huấn luyện lại mô hình Data Mining đều được tự động hóa trên một pipeline duy nhất. Dữ liệu OWID (mã ISO-3, hơn 393.000 dòng) và WHO (mã ISO-2) được giữ đúng mức độ chi tiết (grain) riêng biệt để tránh join sai mã quốc gia. Ở nhánh OLTP, ngay từ bước xuất raw `patients.csv` lên MinIO, pipeline đã chủ động lược bỏ cột họ tên thật `full_name`."

---

## 3. Bài toán Data Mining 1 & 3: Phân cụm K-Means, Chọn số cụm K, PCA & Chuỗi Markov | 3 phút

**Thao tác:**
1. Chuyển sang **Tab 3 (`Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu` — 4 biểu đồ)** và **Tab 4 (`Phân tích Phân cụm Dịch tễ COVID-19 — Đánh giá Số cụm K, PCA & Chuyển cụm Markov` — 8 biểu đồ)** trên Superset.
2. Chỉ vào 2 biểu đồ kiểm định K ở Phần 1, tiếp theo là **Bản đồ Phân bố Cụm trên Không gian 2 Chiều (PCA)**, **Bảng Hồ sơ Thống kê & Nhãn Dịch tễ Tự sinh**, rồi cuộn xuống Phần 3 chỉ vào **Ma trận Xác suất Chuyển dịch Cụm Markov (%)** và **Bảng Thời gian Lưu trú Liên tục (Dwell Time)**.
3. *(Tùy chọn)* Chạy nhanh trên PowerShell để in bảng đối chiếu lịch sử các đợt dịch của Việt Nam:
   ```powershell
   python .\scripts\demo_kmeans.py
   ```

**Lời dẫn gợi ý:**
> "Trên 56.477 quan sát tuần – quốc gia, thay vì chọn cứng `K = 4` theo cảm tính, nhóm chạy kiểm định `K` từ 2 đến 8 bằng 4 chỉ số: **Inertia (Phương pháp Khuỷu tay)**, **Silhouette** (lấy mẫu 10.000 điểm), **Davies–Bouldin** và **Calinski–Harabasz**. Kết quả cho thấy điểm gãy khuỷu tay xuất hiện rõ ở `K = 4 – 5` (`K = 5` đạt Silhouette cao nhất `0,5821`, trong khi `K = 4` cho 4 nhóm dịch tễ rất trực quan và ổn định).  
> Hệ thống tự động giải chuẩn hóa tâm cụm để sinh nhãn tiếng Việt: *Thấp – ổn định* (62,4%), *Trung bình – ổn định* (26,4%), *Cao – đang tăng* (1,4% — trạng thái chuyển tiếp ngắn chỉ lưu trú trung bình `1,02 tuần/đợt` theo thống kê **Markov Dwell Time**) và *Rất cao – ổn định* (9,9% — mỗi đợt đỉnh dịch kéo dài trung bình `5,65 tuần` liên tiếp với xác suất giữ cụm `82,30%`)."

---

## 4. Bài toán Data Mining 2 & 5a: Cảnh báo Sớm XGBoost + TreeSHAP & Bất thường Isolation Forest | 3 phút

**Thao tác:**
1. Chuyển sang **Tab 5 (`Hệ thống Cảnh báo Sớm Bùng phát (XGBoost + TreeSHAP) & Bất thường (Isolation Forest)` — 7 biểu đồ)**.
2. Trình bày lần lượt:
   - **Bản đồ Thế giới: Xác suất Bùng phát Dịch trong 2 Tuần tới** & **Tỷ lệ Quốc gia theo Mức Cảnh báo Sớm** (`Nguy cơ cao`, `Cần theo dõi`, `Nguy cơ thấp`).
   - **Đối sánh Độ chính xác của 3 Mô hình Dự báo trên Tập Kiểm thử** & **10 Yếu tố Ảnh hưởng Lớn nhất**.
   - **Bảng Xếp hạng Cảnh báo Sớm theo Quốc gia** (chỉ vào cột **Giải thích TreeSHAP — 3 yếu tố chính**).
   - Cuộn xuống Phần 3 chỉ vào biểu đồ Donut và **Bảng Chi tiết các Tuần Bất thường Dịch tễ Toàn cầu (Isolation Forest)**.

**Lời dẫn gợi ý:**
> "Phân cụm K-Means chỉ mô tả hiện tại, nên nhóm phát triển tiếp mô hình phân lớp nhị phân cảnh báo trước **2 tuần**: liệu ca mắc có tăng từ 50% trở lên và vượt ngưỡng 10 ca/100.000 dân hay không.  
> Nhóm xây dựng 18 đặc trưng nhân quả chỉ dùng thông tin đến tuần `t` (lag 1–3 tuần, trung bình/độ lệch chuẩn trượt 4 tuần, vận tốc & gia tốc lây nhiễm, CFR trượt 4 tuần, độ phủ vắc-xin, chu kỳ mùa vụ và nhãn `cluster_id` từ K-Means).  
> Đặc biệt, tập huấn luyện (44.520 dòng) và tập kiểm thử (11.006 dòng) được chia theo thời gian kèm **khoảng trống purge 2 tuần** để triệt tiêu hoàn toàn hiện tượng rò rỉ nhãn tương lai. Kết quả trên tập kiểm thử độc lập cho thấy **XGBoost** vượt trội cả Hồi quy Logistic lẫn Luật cơ sở: **ROC-AUC đạt 0,925**, **PR-AUC đạt 0,270** (gấp gần 4 lần luật cơ sở `0,070`) và **F1 đạt 0,356**."

---

## 5. Minh họa CDC Realtime, Phát hiện Ổ dịch Kafka & Cổng Web AI Tương tác | 3 phút

**Thao tác:**
1. Mở **Tab 6 (`Giám sát Thời gian thực — Hồ sơ Bệnh nhân Ẩn danh & Tiêm chủng Nội bộ`)**, ghi nhớ con số trên thẻ **Tổng số Hồ sơ Bệnh nhân Đã Đồng bộ Ẩn danh** và **Tổng số Mũi tiêm**.
2. Mở cửa sổ PowerShell, chạy lệnh phát sinh giao dịch thật + giả lập ổ dịch bùng phát tức thời vào MySQL OLTP:
   ```powershell
   # 1) Đồng bộ CDC + Ẩn danh hóa bệnh nhân mới (< 2 giây)
   .\scripts\demo_cdc.ps1 -Count 2

   # 2) Bơm burst 12 ca dương tính tại Hà Nội -> Kafka Streaming Outbreak Detector tự bắn cảnh báo (< 3 giây)
   .\scripts\demo_realtime_outbreak.ps1 -Province "Hà Nội" -BurstCount 12
   ```
3. Cho hội đồng thấy kết quả trên PowerShell xuất hiện trong PostgreSQL DWH (`dwh.dim_patient` không có `full_name`/`citizen_hash`, `dwh.fact_realtime_anomalies` ghi nhận `Z-score` vượt ngưỡng với mức `CẢNH BÁO CAO`), sau đó bấm **Refresh** trên Tab 6 Superset (`9 biểu đồ`) để thấy KPI bệnh nhân/mũi tiêm nhảy số và **Bảng Cảnh báo Ổ dịch Bất thường Thời gian thực (Kafka CDC)** ở Phần 3 cập nhật ngay lập tức!
4. Mở trình duyệt sang **Cổng Web AI Tương tác Streamlit** tại `http://localhost:8501` để cho hội đồng tự trải nghiệm trực tiếp 4 tab:
   - **Tab 1 — Mô phỏng Kịch bản What-If (XGBoost + TreeSHAP):** Kéo thanh trượt tỷ lệ tiêm chủng, ca mắc tuần hiện tại, vận tốc lây nhiễm → bấm chạy trực tiếp trên mô hình `early_warning_model.pkl` để xem xác suất bùng phát thay đổi tức thì kèm **biểu đồ Waterfall TreeSHAP** giải thích từng yếu tố kéo tăng/giảm nguy cơ.
   - **Tab 2 — Đánh giá Rủi ro Bệnh nhân (Luật kết hợp Apriori):** Chọn nhóm tuổi, bệnh nền, trạng thái tiêm chủng → so khớp ngay với bảng luật `dwh.fact_patient_association_rules` (Support, Confidence, Lift).
   - **Tab 3 — Quỹ đạo Dịch & Dự báo Markov ($P^k$):** Chọn bất kỳ quốc gia nào (vd. Việt Nam, Mỹ, Ấn Độ) để xem chuỗi chuyển cụm theo thời gian và phân phối xác suất sau $k$ tuần bằng lũy thừa ma trận Markov.
   - **Tab 4 — Bất thường Lịch sử (Isolation Forest) & Ổ dịch Thời gian thực (Kafka):** Xem ngay cảnh báo ổ dịch Hà Nội vừa tạo từ lệnh PowerShell và 1.130 tuần bất thường lịch sử toàn cầu.

**Lời dẫn gợi ý:**
> "Không chỉ dừng lại ở dashboard báo cáo tĩnh, hệ thống còn tích hợp luồng **CDC thời gian thực kết hợp phát hiện ổ dịch trực tuyến (Streaming Outbreak Detector)** ngay trên Kafka: khi Hà Nội đột ngột ghi nhận cụm ca dương tính trong cửa sổ trượt, hệ thống tính Z-score và ghi cảnh báo vào DWH trong chưa đầy 3 giây, đồng thời vẫn ẩn danh hoàn toàn PII của bệnh nhân.  
> Đặc biệt, với **Cổng Web AI Tương tác (Streamlit)** tại cổng `8501`, giảng viên và chuyên gia dịch tễ có thể tự thay đổi giả định đầu vào (What-If) để chạy suy luận trực tiếp trên mô hình XGBoost thật kèm giải thích cục bộ **TreeSHAP** cho từng yếu tố."

---

## 6. Câu hỏi Phản biện Thường gặp & Gợi ý Trả lời Nhanh

- **Hỏi: Đưa `cluster_id` của K-Means (vốn fit trên toàn bộ lịch sử) vào XGBoost có làm phồng kết quả kiểm thử do rò rỉ dữ liệu không?**  
  **Đáp:** Nhóm đã kiểm chứng bằng thực nghiệm **Ablation** (`COVID_EW_INCLUDE_CLUSTER=false`). Khi bỏ hoàn toàn `cluster_id`, XGBoost vẫn đạt `ROC-AUC = 0,926`, `F1 = 0,355`, `PR-AUC = 0,264` (so với `0,270` khi có `cluster_id`). Điều này chứng minh mô hình không hề bị phồng điểm ảo, và `cluster_id` đóng góp thêm tín hiệu phân tầng hợp lệ.
- **Hỏi: Vì sao PR-AUC (`0,270`) thấp hơn nhiều so với ROC-AUC (`0,925`)?**  
  **Đáp:** Tập test nằm ở giai đoạn cuối (từ cuối 2023 sang 2024), khi phần lớn quốc gia đã qua đỉnh đại dịch và giảm tần suất báo cáo nên tỷ lệ tuần bùng phát thực sự (lớp dương) rất hiếm. Với dữ liệu mất cân bằng mạnh, PR-AUC luôn khắt khe hơn ROC-AUC; việc XGBoost đạt `PR-AUC = 0,270` so với baseline ngẫu nhiên/luật cơ sở (`0,070`) thể hiện khả năng cô lập nguy cơ cao gấp ~4 lần.
- **Hỏi: Mô hình XGBoost bị coi là hộp đen (black-box), làm sao bác sĩ/nhà quản lý tin tưởng cảnh báo của từng quốc gia?**  
  **Đáp:** Bên cạnh Feature Importance toàn cục, nhóm tích hợp **TreeSHAP cục bộ** (`pred_contribs=True` chuẩn xác trên cây quyết định XGBoost) để xuất cột `top_reasons_vi` giải thích trực tiếp 3 yếu tố đóng góp mạnh nhất cho từng quốc gia, đồng thời cho phép kiểm chứng trực quan bằng biểu đồ Waterfall ngay trên Cổng AI Streamlit.
- **Hỏi: Nếu mũi tiêm trên Kafka đến trước bản ghi bệnh nhân thì sao?**  
  **Đáp:** Do `patients` và `vaccination_records` nằm ở 2 topic Kafka độc lập nên thứ tự không được đảm bảo giữa các bảng. Consumer xử lý bằng cơ chế **Placeholder Dimension** (`is_placeholder = TRUE`) để không làm gãy khóa ngoại `fact_vaccination.patient_id`, sau đó khi event bệnh nhân tới sẽ tự động `ON CONFLICT DO UPDATE` ghi đè đầy đủ thuộc tính và đổi `is_placeholder = FALSE`.

---

## Phụ lục: Các lệnh kiểm tra nhanh khi cần

```powershell
# Kiểm tra trạng thái connector CDC v2 & Cổng AI Streamlit
curl.exe http://localhost:8083/connectors/vaccination-oltp-connector-v2/status
curl.exe http://localhost:8501/_stcore/health

# Chạy toàn bộ 11 unit test CDC (kèm Streaming Outbreak) + 17 unit test ML (K-Means, XGBoost, Markov, Apriori, Isolation Forest)
python -m unittest discover -s cdc -p "test_*.py" -v
docker exec -i airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v

# Sinh thêm dữ liệu bệnh nhân/tiêm chủng giả lập vào MySQL OLTP
python .\scripts\seed_oltp_data.py --patients 2500
```
