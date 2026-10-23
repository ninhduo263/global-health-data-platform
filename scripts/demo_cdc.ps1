# ==============================================================================
# Script Demo CDC Realtime: MySQL -> Debezium -> Kafka -> PostgreSQL DWH
# Chạy trực tiếp qua Docker, không cần cài đặt Python hay thư viện trên máy Host.
# ==============================================================================

param(
    [int]$Count = 3
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "       KỊCH BẢN DEMO CDC STREAMING: MySQL -> Kafka -> PostgreSQL DWH" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$vaccines = @("Pfizer-BioNTech", "AstraZeneca", "Moderna", "VeroCell")
$centers = @(1, 2, 3)

for ($i = 1; $i -le $Count; $i++) {
    $citizenHash = "citizen_demo_" + (Get-Random -Minimum 10000 -Maximum 99999)
    $centerId = $centers[(Get-Random -Minimum 0 -Maximum $centers.Length)]
    $vaccine = $vaccines[(Get-Random -Minimum 0 -Maximum $vaccines.Length)]
    $dose = (Get-Random -Minimum 1 -Maximum 4)
    $lot = "LOT-DEMO-" + (Get-Random -Minimum 100 -Maximum 999)
    $today = (Get-Date).ToString("yyyy-MM-dd")

    Write-Host "[$i/$Count] BƯỚC 1: Phát sinh giao dịch tiêm chủng trên MySQL OLTP..." -ForegroundColor Green
    $insertQuery = "INSERT INTO vaccination_records (citizen_hash, center_id, country_code, vaccine_type, dose_number, batch_number, vaccination_date) VALUES ('$citizenHash', $centerId, 'VNM', '$vaccine', $dose, '$lot', '$today'); SELECT LAST_INSERT_ID();"
    
    $recordIdOutput = docker exec -i mysql-oltp mysql -uroot -proot_password -N -e "$insertQuery" vaccination_oltp 2>$null
    $recordId = $recordIdOutput.Trim()
    
    Write-Host "  -> Đã ghi nhận mũi tiêm: Record ID = $recordId | Vaccine = $vaccine (Mũi $dose) | Điểm tiêm = $centerId" -ForegroundColor Gray
    Write-Host "  -> Đang đợi Debezium bắt binlog và Kafka chuyển tiếp (< 2 giây)..." -ForegroundColor DarkYellow
    Start-Sleep -Seconds 2

    Write-Host "[$i/$Count] BƯỚC 2: Kiểm tra dữ liệu tại Data Warehouse (PostgreSQL)..." -ForegroundColor Cyan
    $checkQuery = "SELECT record_id, center_id, vaccine_type, dose_number, to_char(updated_at, 'HH24:MI:SS') FROM dwh.fact_vaccination WHERE record_id = $recordId;"
    $dwhResult = docker exec -i postgres-dwh psql -U dwh_admin -d health_dwh -t -c "$checkQuery" 2>$null

    if ($dwhResult -and $dwhResult.Trim() -ne "") {
        Write-Host "  -> [CDC THÀNH CÔNG] Dữ liệu đã xuất hiện ngay tức thì trong dwh.fact_vaccination:" -ForegroundColor Green
        Write-Host "     $($dwhResult.Trim())" -ForegroundColor White
    } else {
        Write-Host "  -> [CDC ĐANG XỬ LÝ] Vui lòng kiểm tra lại sau 1-2 giây." -ForegroundColor Yellow
    }
    Write-Host ""
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  HOÀN THÀNH DEMO CDC STREAMING REALTIME!" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan

