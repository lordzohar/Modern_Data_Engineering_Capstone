# =============================================================================
# Captures every README screenshot via headless Microsoft Edge.
# Prerequisite: stack is up (`bootstrap.cmd`) and the 6 pipeline services are
# running (`bootstrap.cmd --launch-panes` or `start_pipeline.ps1`).
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File assets\capture_screenshots.ps1
# =============================================================================
$ErrorActionPreference = 'Stop'

$edge = "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe"
if (-not (Test-Path $edge)) { $edge = "${env:ProgramFiles}\Microsoft\Edge\Application\msedge.exe" }
if (-not (Test-Path $edge)) { throw "Microsoft Edge not found. Install Edge or adapt this script for Chrome." }

$root  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$shots = Join-Path $root 'screenshots'
if (-not (Test-Path $shots)) { New-Item -ItemType Directory -Path $shots | Out-Null }

$slug = 'taxi-kraft-cluster'

function Shoot {
    param([string]$Url, [string]$File, [int]$Delay = 12000, [int]$W = 1600, [int]$H = 1100)
    $out = Join-Path $shots $File
    Remove-Item $out -ErrorAction SilentlyContinue
    & $edge --headless=new --disable-gpu --hide-scrollbars `
        --window-size="$W,$H" --virtual-time-budget=$Delay `
        --screenshot="$out" $Url 2>$null | Out-Null
    if (Test-Path $out) {
        Write-Host ("{0,-32} {1,9:N0} bytes" -f $File, (Get-Item $out).Length)
    } else {
        Write-Host "FAIL $File"
    }
}

# --- Live dashboard ---------------------------------------------------------
Shoot 'http://localhost:5000'                                                       'dashboard_live_map.png'        15000

# --- Kafka UI ---------------------------------------------------------------
Shoot "http://localhost:8080/ui/clusters/$slug"                                     'kafka_ui_overview.png'
Shoot "http://localhost:8080/ui/clusters/$slug/brokers"                             'kafka_ui_brokers.png'
Shoot "http://localhost:8080/ui/clusters/$slug/all-topics"                          'kafka_ui_topics.png'
Shoot "http://localhost:8080/ui/clusters/$slug/all-topics/taxi-trips/messages"      'kafka_ui_topic_messages.png'   15000
Shoot "http://localhost:8080/ui/clusters/$slug/consumer-groups"                     'kafka_ui_consumer_groups.png'
Shoot "http://localhost:8080/ui/clusters/$slug/schemas"                             'kafka_ui_schemas.png'
Shoot "http://localhost:8080/ui/clusters/$slug/connectors"                          'kafka_ui_connectors.png'
Shoot "http://localhost:8080/ui/clusters/$slug/ksqldb/streams"                      'kafka_ui_ksqldb_streams.png'
Shoot "http://localhost:8080/ui/clusters/$slug/ksqldb/tables"                       'kafka_ui_ksqldb_tables.png'

# --- Prometheus -------------------------------------------------------------
Shoot 'http://localhost:9090/targets'                                               'prometheus_targets.png'
Shoot 'http://localhost:9090/alerts'                                                'prometheus_alert_rules.png'
Shoot 'http://localhost:9090/graph?g0.expr=rate(kafka_server_brokertopicmetrics_messagesin_total%5B1m%5D)&g0.tab=0&g0.range_input=10m' 'prometheus_messages_in.png'

# --- Alertmanager (push two demo alerts first so the screen is informative) -
$demo = Join-Path $root 'demo_alerts.json'
@'
[
  {"labels":{"alertname":"DemoSurgeActive","severity":"info","zone":"MIDTOWN","instance":"surge-detector"},
   "annotations":{"summary":"Surge pricing active in MIDTOWN","description":"Multiplier: 3.00x"}},
  {"labels":{"alertname":"DemoHighConsumerLag","severity":"warning","topic":"gps-pings","group":"dash-realtime"},
   "annotations":{"summary":"Consumer group dash-realtime is lagging","description":"Lag is 1248 on topic gps-pings"}}
]
'@ | Set-Content -Path $demo -Encoding ASCII
try {
    Invoke-RestMethod -Method Post -Uri http://localhost:9093/api/v2/alerts `
        -ContentType 'application/json' -Body (Get-Content $demo -Raw) | Out-Null
    Start-Sleep -Seconds 2
} catch { Write-Host "  (could not push demo alerts: $_)" }
Remove-Item $demo -ErrorAction SilentlyContinue

Shoot 'http://localhost:9093/#/alerts' 'alertmanager.png'
Shoot 'http://localhost:9093/#/status' 'alertmanager_status.png'

# --- Grafana ---------------------------------------------------------------
Shoot 'http://localhost:3000/?orgId=1' 'grafana_home.png'

# --- Connect REST snapshot --------------------------------------------------
Shoot 'http://localhost:8083/connectors/drivers-postgres-source/status' 'connect_status.png' 5000

Write-Host ''
Write-Host 'Done. Final inventory:'
Get-ChildItem $shots | Sort-Object Name | Format-Table Name, Length
