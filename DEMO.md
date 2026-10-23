# Kịch bản demo project

Thời lượng gợi ý: 8-10 phút. Luồng demo: OWID/WHO/MySQL → MinIO → DWH → K-means → Superset; cuối cùng minh họa CDC MySQL riêng.

## Chuẩn bị trước

- Mở PowerShell tại thư mục project.
- Khởi động dịch vụ và xác nhận các container healthy:

```powershell
docker compose up -d
docker compose ps
```

- Nếu đây là database volume cũ, chạy migration và khởi động lại Airflow để nạp schema/dependency mới:

```powershell
docker compose up -d --force-recreate dwh-migrate
docker compose logs --tail 30 dwh-migrate
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

- Đăng nhập Airflow `http://localhost:8081` và Superset `http://localhost:8089`.
- Trước buổi demo, thêm các dataset Superset `vw_dashboard_cases`, `vw_who_covid_daily`, `vw_weekly_covid_clusters`, `vw_dashboard_vaccinations`; tạo sẵn chart nếu muốn chuyển cảnh nhanh.
- Nếu có thể, trigger DAG trước buổi trình bày một lần. OWID là snapshot lớn, tải toàn bộ có thể mất thời gian.

## 1. Mở đầu | 30 giây

**Nói:**

> Project xây dựng một data platform cho dữ liệu y tế công cộng. Dữ liệu COVID-19 toàn cầu được lấy theo batch từ OWID, dữ liệu WHO được lọc theo cấu hình; dữ liệu giao dịch tiêm chủng nội bộ được đồng bộ riêng bằng CDC. Sau khi vào DWH, dữ liệu OWID theo tuần được phân cụm K-means và hiển thị trên Superset.

**Cho xem:** sơ đồ kiến trúc trong README hoặc lần lượt các service trong `docker compose ps`.

## 2. Chạy pipeline batch | 2 phút

1. Mở DAG `health_data_pipeline` trong Airflow.
2. Chọn **Trigger DAG** và mở Graph/Grid để theo dõi các task.
3. Khi hoàn tất, mở MinIO Console `http://localhost:9001`, vào bucket `covid19-landing-zone`.

**Nói:**

> Airflow tải snapshot OWID, lọc file WHO và trích xuất hai bảng OLTP. Bản raw được lưu theo ngày trong MinIO. Transform đọc lại OWID raw từ MinIO để nạp fact quốc gia/ngày; WHO được lưu ở fact riêng vì mã quốc gia WHO là ISO-2, khác ISO-3 của OWID.

**Cho xem các prefix:** `raw/owid/`, `raw/who/`, `raw/oltp/`.

## 3. Kiểm tra dữ liệu DWH | 1 phút

Trong Superset, mở SQL Lab trên database **Health DWH** và chạy:

```sql
SELECT COUNT(*) AS observations,
       COUNT(DISTINCT country_code) AS countries,
       MIN(report_date) AS first_date,
       MAX(report_date) AS last_date
FROM dwh.vw_dashboard_cases;
```

Để chứng minh WHO được lưu riêng:

```sql
SELECT who_region,
       COUNT(*) AS observations,
       MIN(report_date) AS first_date,
       MAX(report_date) AS last_date
FROM dwh.vw_who_covid_daily
GROUP BY who_region
ORDER BY who_region;
```

**Nói:**

> DWH giữ dữ liệu theo đúng grain và mã quốc gia của từng nguồn. WHO ISO-2 và OWID ISO-3 không bị join nhầm vào cùng một dimension/fact.

## 4. Trình bày K-means | 2 phút

1. Trong Superset, mở chart/dataset `vw_weekly_covid_clusters`.
2. Cho xem số quốc gia theo cụm theo tuần, incidence trung bình theo cụm, rồi bảng chi tiết quốc gia.
3. Có thể chạy truy vấn để xem tuần mới nhất:

```sql
SELECT week_start,
       cluster_id,
       COUNT(DISTINCT country_code) AS countries,
       ROUND(AVG(cases_per_100k), 2) AS avg_cases_per_100k,
       ROUND(AVG(deaths_per_100k), 2) AS avg_deaths_per_100k
FROM dwh.vw_weekly_covid_clusters
WHERE week_start = (SELECT MAX(week_start) FROM dwh.vw_weekly_covid_clusters)
GROUP BY week_start, cluster_id
ORDER BY cluster_id;
```

**Nói:**

> Mỗi điểm dữ liệu là một quốc gia trong một tuần. Thuật toán dùng ca mắc và tử vong trên 100.000 dân cùng biến động ca so với tuần trước; các đặc trưng được chuẩn hóa trước khi chạy K-means. Cluster ID được sắp theo incidence trung bình để dễ đọc, nhưng đây là phân nhóm xu hướng chứ không phải mức nguy cơ y tế đã được chẩn đoán hay dự báo chắc chắn.

## 5. Minh họa CDC | 2 phút

Trong PowerShell chạy:

```powershell
.\scripts\demo_cdc.ps1 -Count 2
```

Script thêm hai giao dịch tiêm chủng thử vào MySQL rồi kiểm tra chúng xuất hiện trong `dwh.fact_vaccination`.

**Nói:**

> CDC là luồng khác với pipeline OWID: Debezium đọc binlog MySQL, Kafka chuyển sự kiện, consumer áp dụng vào PostgreSQL. Consumer commit Kafka offset sau khi transaction DWH thành công và không đưa `citizen_hash` vào DWH.

Nếu bản ghi chưa xuất hiện ngay, kiểm tra log:

```powershell
docker compose logs --tail 30 cdc-consumer
```

## 6. Kết luận | 30 giây

**Nói:**

> Project kết hợp batch ETL cho dữ liệu công khai, CDC cho dữ liệu giao dịch nội bộ, MinIO làm nơi lưu raw và PostgreSQL làm DWH. K-means bổ sung góc nhìn phân nhóm theo tuần. OWID hiện là snapshot chạy theo lịch, không phải nguồn realtime; CDC mới là luồng gần realtime trong project.

## Nếu demo bị chặn

- **DAG đang chạy lâu:** dùng kết quả của lần chạy trước và nói rõ OWID tải toàn bộ snapshot.
- **Dataset Superset chưa có cột:** vào dataset tương ứng, bấm đồng bộ/refresh metadata sau khi migration và DAG hoàn tất.
- **Bảng phân cụm chưa có dữ liệu:** kiểm tra task `weekly_covid_kmeans` trong Airflow; task này chỉ chạy sau `transform_and_load_to_dwh`.
- **CDC chưa đồng bộ:** kiểm tra `docker compose logs --tail 30 cdc-consumer` và trạng thái connector ở `http://localhost:8083/connectors/vaccination-oltp-connector/status`.