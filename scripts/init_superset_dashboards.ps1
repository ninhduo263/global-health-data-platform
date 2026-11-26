# ==============================================================================
# Script Wrapper PowerShell: Tu dong khoi tao Datasets & Dashboards tren Superset
# ==============================================================================
$ScriptPath = Join-Path $PSScriptRoot "init_superset_dashboards.py"
python $ScriptPath @args
