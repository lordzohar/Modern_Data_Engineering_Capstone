# =============================================================================
#  start_pipeline.ps1
#  One-click launcher: opens Windows Terminal with 6 panes, one per pipeline
#  service. Each pane activates the project venv before running its script.
#
#  Usage (after the Docker stack is up via bootstrap.cmd):
#      powershell -ExecutionPolicy Bypass -File .\start_pipeline.ps1
#
#  Layout:
#      +-------------------+-------------------+-------------------+
#      | 1 taxi_simulator  | 3 driver_enricher | 5 quality_validator|
#      +-------------------+-------------------+-------------------+
#      | 2 taxi_consumer   | 4 surge_detector  | 6 dashboard (web) |
#      +-------------------+-------------------+-------------------+
# =============================================================================

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Definition
$venv = Join-Path $here ".venv\Scripts\activate.bat"

if (-not (Test-Path $venv)) {
    Write-Host "ERROR: .venv not found. Run bootstrap.cmd first." -ForegroundColor Red
    exit 1
}

if (-not (Get-Command wt.exe -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Windows Terminal (wt.exe) not found. Install it from the Microsoft Store." -ForegroundColor Red
    exit 1
}

# Each pane: cmd /k <activate venv> && <run script>
function Pane($title, $script) {
    return "cmd /k `"title $title && call `"$venv`" && python $script`""
}

$cmd = "new-tab -d `"$here`" --title `"1 simulator`" " + (Pane "1-simulator"  "taxi_simulator.py --drivers 50") + " ; " +
       "split-pane  -d `"$here`" -V --title `"2 consumer`"  " + (Pane "2-consumer"   "taxi_consumer.py")              + " ; " +
       "split-pane  -d `"$here`" -H --title `"3 enricher`"  " + (Pane "3-enricher"   "driver_enricher.py")            + " ; " +
       "move-focus left ; " +
       "split-pane  -d `"$here`" -H --title `"4 surge`"     " + (Pane "4-surge"      "surge_detector.py")             + " ; " +
       "focus-pane -t 1 ; " +
       "split-pane  -d `"$here`" -H --title `"5 quality`"   " + (Pane "5-quality"    "quality_validator.py")          + " ; " +
       "focus-pane -t 3 ; " +
       "split-pane  -d `"$here`" -V --title `"6 dashboard`" " + (Pane "6-dashboard"  "dashboard.py")

Write-Host "Launching 6 pipeline panes in Windows Terminal..." -ForegroundColor Cyan
Start-Process wt.exe -ArgumentList $cmd

Write-Host ""
Write-Host "Open these in your browser to see the system come alive:" -ForegroundColor Yellow
Write-Host "  Live Taxi Map  -> http://localhost:5000"
Write-Host "  Kafka UI       -> http://localhost:8080"
Write-Host "  Grafana        -> http://localhost:3000  (admin / admin)"
Write-Host "  Prometheus     -> http://localhost:9090"
Write-Host "  Alertmanager   -> http://localhost:9093"
Write-Host "  Connect REST   -> http://localhost:8083"
