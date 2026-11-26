# ==============================================================================
# Script Wrapper PowerShell: Sinh bao cao Word GIOI_THIEU_HE_THONG.docx
# ==============================================================================
$RepoRoot = Split-Path -Parent $PSScriptRoot
$PyScript = Join-Path $PSScriptRoot "generate_docx_report.py"
$StagingOut = Join-Path $RepoRoot "data\GIOI_THIEU_HE_THONG.docx"
$FinalOut = Join-Path $RepoRoot "GIOI_THIEU_HE_THONG.docx"

docker exec -i airflow-scheduler python3 -m pip install --quiet python-docx matplotlib pillow
docker cp $PyScript airflow-scheduler:/tmp/generate_docx_report.py
docker exec -i airflow-scheduler python3 /tmp/generate_docx_report.py --output /opt/airflow/data/GIOI_THIEU_HE_THONG.docx
if ($LASTEXITCODE -eq 0 -and (Test-Path $StagingOut)) {
    Move-Item -Force $StagingOut $FinalOut
    Write-Host "[+] Da cap nhat thanh cong: $FinalOut" -ForegroundColor Green
}
