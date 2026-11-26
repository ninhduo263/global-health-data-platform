# Kịch bản Demo Bảo vệ Đồ án (Data Platform & Data Mining)

Thời lượng gợi ý: **10 – 12 phút**.  
Luồng trình bày xuyên suốt: **Thu thập đa nguồn (OWID/WHO/MySQL) → Data Lake (MinIO) → Kho dữ liệu Snowflake (PostgreSQL) → Phân cụm K-Means nâng cao → Cảnh báo sớm XGBoost → Đồng bộ CDC ẩn danh bệnh nhân → 4 Dashboard tiếng Việt trên Superset**.

---

## 0. Checklist chuẩn bị 5 phút trước giờ bảo vệ

1. **Đảm bảo các container đang chạy ổn định:**
   ```powershell
   docker compose ps
   ```
   *(Nếu vừa bật máy lại: chạy `docker compose up -d`)*

2. **Đồng bộ 4 Dashboard tiếng Việt (21 biểu đồ) trên Superset (nếu cần làm mới):**
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
> (2) **Streaming CDC thời gian thực** qua Debezium và Kafka đồng bộ hồ sơ bệnh nhân và lịch sử tiêm chủng từ MySQL OLTP sang DWH có ẩn danh hóa dữ liệu cá nhân.  
> Trên nền tảng DWH đó, nhóm triển khai chuỗi **2 bài toán Data Mining nối tiếp nhau**: Phân cụm xu hướng dịch tễ K-Means có kiểm định định lượng số cụm K + giảm chiều PCA, và mô hình học máy **XGBoost** cảnh báo sớm nguy cơ bùng phát dịch trước 2 tuần."

---

## 2. Luồng Điều phối Airflow & Data Lake MinIO | 1.5 phút

**Thao tác:**
1. Chuyển sang **Tab 1 (Airflow)** → mở tab **Graph** của DAG `health_data_pipeline`, chỉ vào chuỗi phụ thuộc:  
   `[extract_owid_api, extract_who_csv, extract_mysql_oltp] >> load_to_minio >> transform_and_load_to_dwh >> weekly_covid_kmeans >> covid_early_warning`.
2. Chuyển sang **Tab 2 (MinIO)** → mở `raw/owid/`, `raw/who/`, `raw/oltp/`.

**Lời dẫn gợi ý:**
> "Toàn bộ quy trình từ trích xuất, tiền xử lý đến huấn luyện lại mô hình Data Mining đều được tự động hóa trên một pipeline duy nhất. Dữ liệu OWID (mã ISO-3, hơn 393.000 dòng) và WHO (mã ISO-2) được giữ đúng mức độ chi tiết (grain) riêng biệt để tránh join sai mã quốc gia. Ở nhánh OLTP, ngay từ bước xuất raw `patients.csv` lên MinIO, pipeline đã chủ động lược bỏ cột họ tên thật `full_name`."

---

## 3. Bài toán Data Mining 1: Phân cụm K-Means, Chọn số cụm K & PCA | 3 phút

**Thao tác:**
1. Chuyển sang **Tab 3 (`Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu`)** và **Tab 4 (`Phân tích Phân cụm Dịch tễ COVID-19 — Đánh giá Số cụm K & Hồ sơ Cụm`)** trên Superset.
2. Chỉ vào 2 biểu đồ kiểm định K ở Phần 1, tiếp theo là **Bản đồ Phân bố Cụm trên Không gian 2 Chiều (PCA)** và **Bảng Hồ sơ Thống kê & Nhãn Dịch tễ Tự sinh**.
3. *(Tùy chọn)* Chạy nhanh trên PowerShell để in bảng đối chiếu lịch sử các đợt dịch của Việt Nam:
   ```powershell
   python .\scripts\demo_kmeans.py
   ```

**Lời dẫn gợi ý:**
> "Trên 56.477 quan sát tuần – quốc gia, thay vì chọn cứng `K = 4` theo cảm tính, nhóm chạy kiểm định `K` từ 2 đến 8 bằng 4 chỉ số: **Inertia (Phương pháp Khuỷu tay)**, **Silhouette** (lấy mẫu 10.000 điểm), **Davies–Bouldin** và **Calinski–Harabasz**. Kết quả cho thấy điểm gãy khuỷu tay xuất hiện rõ ở `K = 4 – 5` (`K = 5` đạt Silhouette cao nhất `0,5821`, trong khi `K = 4` cho 4 nhóm dịch tễ rất trực quan và ổn định).  
> Hệ thống tự động giải chuẩn hóa tâm cụm để sinh nhãn tiếng Việt: *Thấp – ổn định* (62,4%), *Trung bình – ổn định* (26,4%), *Cao – đang tăng* (1,4% — bắt đúng các tuần bùng phát đột biến +705%/tuần) và *Rất cao – ổn định* (9,9%). Đồng thời, phép chiếu **PCA 2 chiều** cho thấy ranh giới phân tách rõ rệt giữa 4 cụm. Khi đối chiếu Việt Nam, đỉnh sóng Delta tháng 8/2021 rơi đúng vào cụm *Rất cao*, còn đỉnh Omicron tháng 3/2022 nhờ độ phủ vắc-xin cao nên tỷ lệ tử vong thấp, chỉ nằm ở cụm *Trung bình*."

---

## 4. Bài toán Data Mining 2: Cảnh báo Sớm Bùng phát Dịch bằng XGBoost | 3 phút

**Thao tác:**
1. Chuyển sang **Tab 5 (`Hệ thống Cảnh báo Sớm Nguy cơ Bùng phát Dịch COVID-19`)**.
2. Trình bày lần lượt:
   - **Bản đồ Thế giới: Xác suất Bùng phát Dịch trong 2 Tuần tới** & **Tỷ lệ Quốc gia theo Mức Cảnh báo Sớm** (`Nguy cơ cao`, `Cần theo dõi`, `Nguy cơ thấp`).
   - **Đối sánh Độ chính xác của 3 Mô hình Dự báo trên Tập Kiểm thử**.
   - **10 Yếu tố Ảnh hưởng Lớn nhất đến Nguy cơ Bùng phát Dịch**.
   - **Bảng Xếp hạng Cảnh báo Sớm theo Quốc gia**.

**Lời dẫn gợi ý:**
> "Phân cụm K-Means chỉ mô tả hiện tại, nên nhóm phát triển tiếp mô hình phân lớp nhị phân cảnh báo trước **2 tuần**: liệu ca mắc có tăng từ 50% trở lên và vượt ngưỡng 10 ca/100.000 dân hay không.  
> Nhóm xây dựng 18 đặc trưng nhân quả chỉ dùng thông tin đến tuần `t` (lag 1–3 tuần, trung bình/độ lệch chuẩn trượt 4 tuần, vận tốc & gia tốc lây nhiễm, CFR trượt 4 tuần, độ phủ vắc-xin, chu kỳ mùa vụ và nhãn `cluster_id` từ K-Means).  
> Đặc biệt, tập huấn luyện (44.520 dòng) và tập kiểm thử (11.006 dòng) được chia theo thời gian kèm **khoảng trống purge 2 tuần** để triệt tiêu hoàn toàn hiện tượng rò rỉ nhãn tương lai. Kết quả trên tập kiểm thử độc lập cho thấy **XGBoost** vượt trội cả Hồi quy Logistic lẫn Luật cơ sở: **ROC-AUC đạt 0,925**, **PR-AUC đạt 0,270** (gấp gần 4 lần luật cơ sở `0,070`) và **F1 đạt 0,356**."

---

## 5. Minh họa CDC Thời gian thực & Ẩn danh hóa Bệnh nhân | 2 phút

**Thao tác:**
1. Mở **Tab 6 (`Giám sát Thời gian thực — Hồ sơ Bệnh nhân Ẩn danh & Tiêm chủng Nội bộ`)**, ghi nhớ con số trên thẻ **Tổng số Hồ sơ Bệnh nhân Đã Đồng bộ Ẩn danh** và **Tổng số Mũi tiêm**.
2. Mở cửa sổ PowerShell, chạy lệnh phát sinh giao dịch thật trực tiếp vào MySQL OLTP:
   ```powershell
   .\scripts\demo_cdc.ps1 -Count 2
   ```
3. Cho hội đồng thấy kết quả trên PowerShell xuất hiện trong PostgreSQL DWH chỉ sau ~2 giây (không có `full_name` và `citizen_hash`), sau đó bấm **Refresh** trên Tab 6 Superset để thấy tổng số bệnh nhân và mũi tiêm tăng lên ngay lập tức!

**Lời dẫn gợi ý:**
> "Cuối cùng là luồng đồng bộ thời gian thực phục vụ dữ liệu vận hành nội bộ. Khi phát sinh 2 bệnh nhân và 2 mũi tiêm mới trong MySQL OLTP, Debezium đọc binlog đẩy qua Kafka và `cdc-consumer` cập nhật vào `dwh.dim_patient` cùng `dwh.fact_vaccination` trong chưa đầy 2 giây. Dữ liệu định danh nhạy cảm như họ tên và mã công dân được lọc bỏ hoàn toàn trước khi vào DWH, chỉ giữ lại năm sinh, giới tính, tỉnh thành và bệnh nền để phân tích dịch tễ an toàn."

---

## 6. Câu hỏi Phản biện Thường gặp & Gợi ý Trả lời Nhanh

- **Hỏi: Đưa `cluster_id` của K-Means (vốn fit trên toàn bộ lịch sử) vào XGBoost có làm phồng kết quả kiểm thử do rò rỉ dữ liệu không?**  
  **Đáp:** Nhóm đã kiểm chứng bằng thực nghiệm **Ablation** (`COVID_EW_INCLUDE_CLUSTER=false`). Khi bỏ hoàn toàn `cluster_id`, XGBoost vẫn đạt `ROC-AUC = 0,926`, `F1 = 0,355`, `PR-AUC = 0,264` (so với `0,270` khi có `cluster_id`). Điều này chứng minh mô hình không hề bị phồng điểm ảo, và `cluster_id` đóng góp thêm tín hiệu phân tầng hợp lệ.
- **Hỏi: Vì sao PR-AUC (`0,270`) thấp hơn nhiều so với ROC-AUC (`0,925`)?**  
  **Đáp:** Tập test nằm ở giai đoạn cuối (từ cuối 2023 sang 2024), khi phần lớn quốc gia đã qua đỉnh đại dịch và giảm tần suất báo cáo nên tỷ lệ tuần bùng phát thực sự (lớp dương) rất hiếm. Với dữ liệu mất cân bằng mạnh, PR-AUC luôn khắt khe hơn ROC-AUC; việc XGBoost đạt `PR-AUC = 0,270` so với baseline ngẫu nhiên/luật cơ sở (`0,070`) thể hiện khả năng cô lập nguy cơ cao gấp ~4 lần.
- **Hỏi: Nếu mũi tiêm trên Kafka đến trước bản ghi bệnh nhân thì sao?**  
  **Đáp:** Do `patients` và `vaccination_records` nằm ở 2 topic Kafka độc lập nên thứ tự không được đảm bảo giữa các bảng. Consumer xử lý bằng cơ chế **Placeholder Dimension** (`is_placeholder = TRUE`) để không làm gãy khóa ngoại `fact_vaccination.patient_id`, sau đó khi event bệnh nhân tới sẽ tự động `ON CONFLICT DO UPDATE` ghi đè đầy đủ thuộc tính và đổi `is_placeholder = FALSE`.

---

## Phụ lục: Các lệnh kiểm tra nhanh khi cần

```powershell
# Kiểm tra trạng thái connector CDC v2
curl.exe http://localhost:8083/connectors/vaccination-oltp-connector-v2/status

# Chạy toàn bộ 10 unit test CDC + 13 unit test ML (K-Means & XGBoost)
python -m unittest discover -s cdc -p "test_*.py" -v
docker exec -i airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v

# Sinh thêm dữ liệu bệnh nhân/tiêm chủng giả lập vào MySQL OLTP
python .\scripts\seed_oltp_data.py --patients 2500
```
