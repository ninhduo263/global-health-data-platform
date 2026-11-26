# ==============================================================================
# Script Demo CDC Realtime: MySQL -> Debezium -> Kafka -> PostgreSQL DWH
# Đồng bộ cả bảng patients (ẩn danh sang dim_patient) và vaccination_records.
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
$provinces = @("Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng")
$statuses = @("HEALTHY", "SUSPECTED", "CONFIRMED_COVID", "RECOVERED")
$comorbidities = @("None", "Hypertension", "Diabetes", "Cardiovascular")
$genders = @("M", "F")

for ($i = 1; $i -le $Count; $i++) {
    $randId = (Get-Random -Minimum 100000 -Maximum 999999)
    $citizenHash = "citizen_demo_$randId"
    $fullName = "Nguyen Demo $randId"
    $birthYear = (Get-Random -Minimum 1955 -Maximum 2005)
    $gender = $genders[(Get-Random -Minimum 0 -Maximum $genders.Length)]
    $province = $provinces[(Get-Random -Minimum 0 -Maximum $provinces.Length)]
    $status = $statuses[(Get-Random -Minimum 0 -Maximum $statuses.Length)]
    $comorb = $comorbidities[(Get-Random -Minimum 0 -Maximum $comorbidities.Length)]
    $centerId = $centers[(Get-Random -Minimum 0 -Maximum $centers.Length)]
    $vaccine = $vaccines[(Get-Random -Minimum 0 -Maximum $vaccines.Length)]
    $dose = (Get-Random -Minimum 1 -Maximum 4)
    $lot = "LOT-DEMO-" + (Get-Random -Minimum 100 -Maximum 999)
    $today = (Get-Date).ToString("yyyy-MM-dd")

    Write-Host "[$i/$Count] BƯỚC 1: Phát sinh bệnh nhân + mũi tiêm mới trên MySQL OLTP..." -ForegroundColor Green
    $insertPatient = "INSERT INTO patients (citizen_hash, full_name, birth_year, gender, province, country_code, health_status, comorbidity, reported_date) VALUES ('$citizenHash', '$fullName', $birthYear, '$gender', '$province', 'VNM', '$status', '$comorb', '$today'); SELECT LAST_INSERT_ID();"
    $patientId = (docker exec -i mysql-oltp mysql -uroot -proot_password -N -e "$insertPatient" vaccination_oltp 2>$null).Trim()

    $insertVax = "INSERT INTO vaccination_records (patient_id, citizen_hash, center_id, country_code, vaccine_type, dose_number, batch_number, vaccination_date) VALUES ($patientId, '$citizenHash', $centerId, 'VNM', '$vaccine', $dose, '$lot', '$today'); SELECT LAST_INSERT_ID();"
    $recordId = (docker exec -i mysql-oltp mysql -uroot -proot_password -N -e "$insertVax" vaccination_oltp 2>$null).Trim()

    Write-Host "  -> Đã ghi OLTP: Patient ID = $patientId ($fullName, $birthYear, $comorb) | Record ID = $recordId ($vaccine, Mũi $dose)" -ForegroundColor Gray
    Write-Host "  -> Đang đợi Debezium bắt binlog và Kafka chuyển tiếp (~2 giây)..." -ForegroundColor DarkYellow
    Start-Sleep -Seconds 2

    Write-Host "[$i/$Count] BƯỚC 2: Kiểm tra dữ liệu tại Data Warehouse (PostgreSQL)..." -ForegroundColor Cyan
    $checkPatient = "SELECT patient_id, birth_year, gender, health_status, comorbidity FROM dwh.dim_patient WHERE patient_id = $patientId AND NOT is_placeholder;"
    $patientDwh = docker exec -i postgres-dwh psql -U dwh_admin -d health_dwh -t -c "$checkPatient" 2>$null

    $checkVax = "SELECT record_id, center_id, patient_id, vaccine_type, dose_number, to_char(updated_at, 'HH24:MI:SS') FROM dwh.fact_vaccination WHERE record_id = $recordId;"
    $vaxDwh = docker exec -i postgres-dwh psql -U dwh_admin -d health_dwh -t -c "$checkVax" 2>$null

    if ($patientDwh -and $patientDwh.Trim() -ne "") {
        Write-Host "  -> [dim_patient ẨN DANH] $($patientDwh.Trim()) (không lưu full_name/citizen_hash)" -ForegroundColor Green
    } else {
        Write-Host "  -> [dim_patient] Đang chờ đồng bộ..." -ForegroundColor Yellow
    }

    if ($vaxDwh -and $vaxDwh.Trim() -ne "") {
        Write-Host "  -> [fact_vaccination]    $($vaxDwh.Trim())" -ForegroundColor Green
    } else {
        Write-Host "  -> [fact_vaccination] Đang chờ đồng bộ..." -ForegroundColor Yellow
    }
    Write-Host ""
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  HOÀN THÀNH DEMO CDC STREAMING REALTIME!" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan
