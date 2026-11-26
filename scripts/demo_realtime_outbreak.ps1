# ==============================================================================
# Script Demo Bùng nổ: Mô phỏng Ổ dịch Cục bộ Thời gian thực trên Kafka CDC
# Phát sinh dồn dập các ca CONFIRMED_COVID tại 1 tỉnh vào MySQL OLTP
# -> Debezium -> Kafka -> cdc-consumer phát hiện bất thường Sliding-Window Z-Score
# -> Ghi cảnh báo đỏ vào dwh.fact_realtime_anomalies (< 3 giây)!
# ==============================================================================

param(
    [string]$Province = "Hà Nội",
    [int]$BurstCount = 12
)

Write-Host "=========================================================================" -ForegroundColor Red
Write-Host "   DEMO CẢNH BÁO Ổ DỊCH REALTIME: MySQL -> Kafka -> Streaming Anomaly" -ForegroundColor Yellow
Write-Host "=========================================================================" -ForegroundColor Red
Write-Host "[*] Đang mô phỏng đợt bùng phát $BurstCount ca CONFIRMED_COVID tại '$Province'..." -ForegroundColor Cyan

$today = (Get-Date).ToString("yyyy-MM-dd")
$ts = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

for ($i = 1; $i -le $BurstCount; $i++) {
    $hash = "outbreak_${ts}_${i}_$(Get-Random -Minimum 1000 -Maximum 9999)"
    $sql = "INSERT INTO patients (citizen_hash, full_name, birth_year, gender, province, country_code, health_status, comorbidity, reported_date) VALUES ('$hash', 'Outbreak Case $i', 1962, 'M', '$Province', 'VNM', 'CONFIRMED_COVID', 'Diabetes', '$today');"
    docker exec -i mysql-oltp mysql --default-character-set=utf8mb4 -uroot -proot_password -e "$sql" vaccination_oltp 2>$null
}

Write-Host "[+] Đã phát sinh $BurstCount ca dương tính vào MySQL OLTP. Đang chờ Kafka CDC xử lý (3 giây)..." -ForegroundColor Green
Start-Sleep -Seconds 3

Write-Host "[*] Truy vấn bảng cảnh báo thời gian thực (dwh.vw_realtime_anomalies):" -ForegroundColor Yellow
docker exec -i postgres-dwh psql -U dwh_admin -d health_dwh -c "SELECT alert_id, to_char(detected_at, 'HH24:MI:SS') AS thoi_diem, province AS tinh, confirmed_count AS so_ca, z_score, severity_vi AS muc_do, alert_message_vi AS canh_bao FROM dwh.vw_realtime_anomalies ORDER BY detected_at DESC LIMIT 3;"

Write-Host ">> Mở ngay Cổng AI Portal tại http://localhost:8501 để xem biểu ngữ báo động đỏ!" -ForegroundColor Cyan
