$ErrorActionPreference = 'Continue'
Set-Location 'c:\Users\Gamer\Documents\GitHub\Modern_Data_Engineering_Capstone'

Write-Host '=== 1. Killing host Python pipeline services ==='
$pidDir = 'assets\logs'
if (Test-Path $pidDir) {
    Get-ChildItem "$pidDir\*.pid" -ErrorAction SilentlyContinue | ForEach-Object {
        $procId = (Get-Content $_).Trim()
        if ($procId) {
            try {
                Stop-Process -Id $procId -Force -ErrorAction Stop
                Write-Host ("  killed {0,-22} PID {1}" -f $_.BaseName, $procId)
            } catch {
                Write-Host ("  {0,-22} PID {1} already gone" -f $_.BaseName, $procId)
            }
        }
    }
}
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match 'taxi_simulator|taxi_consumer|driver_enricher|surge_detector|quality_validator|dashboard\.py' } |
    ForEach-Object {
        try { Stop-Process -Id $_.ProcessId -Force; Write-Host ("  killed orphan python PID {0}" -f $_.ProcessId) } catch {}
    }

Write-Host ''
Write-Host '=== 2. docker compose down -v ==='
docker compose down -v --remove-orphans 2>&1 | Select-Object -Last 30

Write-Host ''
Write-Host '=== 3. Containers still running (should be empty for this project) ==='
docker ps --filter 'name=kafka|postgres|connect|schema-registry|ksqldb|kafka-ui|prometheus|grafana|loki|promtail|alertmanager|kafka-lag-exporter|taxi-dashboard' --format 'table {{.Names}}\t{{.Status}}'
