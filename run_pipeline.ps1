
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location -Path $scriptDir

$venvPython = Join-Path $scriptDir ".\.venv\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    Write-Error "Virtual environment Python executable not found: $venvPython. Please ensure .venv is created and activated at least once."
    exit 1 
}

Write-Host "Running pipeline using Python from virtual environment: $venvPython"

$boxIntegrationScript = Join-Path $scriptDir "scripts\box_operations.py"
$db2UploadScript = Join-Path $scriptDir "scripts\db2_data_upload.py"
$mostSoldScript = Join-Path $scriptDir "scripts\most_sold_products.py"
$orderSummaryScript = Join-Path $scriptDir "scripts\order_summary.py"

& $venvPython $boxIntegrationScript

& $venvPython $db2UploadScript

& $venvPython $mostSoldScript

& $venvPython $orderSummaryScript

Write-Host "Pipeline execution complete."
