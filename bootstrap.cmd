@echo off
REM ============================================================================
REM  Modern Data Engineering Capstone - Windows bootstrap
REM
REM  Idempotent. Re-running it:
REM    * skips downloads / venv creation if already present
REM    * removes leftover containers from prior runs (avoids "name in use")
REM    * uses --progress=plain so cmd does not duplicate TTY frames
REM    * optionally launches a 6-pane Windows Terminal with the live pipeline
REM
REM  Usage:
REM      bootstrap.cmd                  -- bring stack up + run setup
REM      bootstrap.cmd --launch-panes   -- also start 6 pipeline terminals
REM ============================================================================
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

set "LAUNCH_PANES=0"
if /I "%~1"=="--launch-panes" set "LAUNCH_PANES=1"
if /I "%~1"=="-p"             set "LAUNCH_PANES=1"

REM --- 1) JMX Prometheus javaagent --------------------------------------------
if not exist "jmx_exporter\jmx_prometheus_javaagent.jar" (
    echo [1/8] Downloading JMX Prometheus javaagent...
    if not exist "jmx_exporter" mkdir jmx_exporter
    curl -fSL -o "jmx_exporter\jmx_prometheus_javaagent.jar" ^
        "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar"
) else (
    echo [1/8] JMX javaagent already present.
)

REM --- 2) Python venv + deps --------------------------------------------------
if not exist ".venv\Scripts\python.exe" (
    echo [2/8] Creating .venv ...
    python -m venv .venv
)
call .venv\Scripts\activate.bat
echo [2/8] Installing Python dependencies (quiet) ...
pip install --quiet --disable-pip-version-check -r requirements.txt

REM --- 3) Remove leftover containers (fixes "name already in use") ------------
echo [3/8] Cleaning up any leftover containers from previous runs...
for %%C in (kafka1 kafka2 kafka3 postgres kafka-connect schema-registry ksqldb-server ksqldb-cli kafka-ui prometheus grafana loki promtail alertmanager kafka-lag-exporter taxi-dashboard) do (
    docker rm -f %%C >nul 2>&1
)

REM --- 4) Bring up the stack (no TTY animation spam) --------------------------
echo [4/8] Starting Kafka KRaft cluster + monitoring stack...
docker compose up -d --progress=plain
if errorlevel 1 (
    echo ERROR: docker compose up failed. Check Docker Desktop is running.
    goto :end
)

REM --- 5) Wait for Kafka ------------------------------------------------------
echo [5/8] Waiting for Kafka cluster to become ready...
set /a count=0
:wait_kafka
docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:29092 >nul 2>&1
if %errorlevel% equ 0 goto kafka_ready
set /a count+=1
if %count% geq 40 goto kafka_timeout
timeout /t 3 /nobreak >nul
goto wait_kafka
:kafka_timeout
echo       WARNING: Kafka not ready after 120s. Check 'docker logs kafka1'.
goto end
:kafka_ready
echo       Kafka cluster is ready.

REM --- 6) Create topics -------------------------------------------------------
echo [6/8] Creating topics...
python setup_topics.py
if errorlevel 1 (
    echo       ERROR: setup_topics.py failed.
    goto end
)

REM --- 7) Seed Postgres + register Debezium -----------------------------------
echo [7/8] Waiting for Postgres...
set /a count=0
:wait_pg
docker exec postgres pg_isready -U taxi >nul 2>&1
if %errorlevel% equ 0 goto pg_ready
set /a count+=1
if %count% geq 20 goto pg_ready
timeout /t 2 /nobreak >nul
goto wait_pg
:pg_ready

echo       Seeding drivers table...
python db_seeder.py
echo       Registering Debezium PostgreSQL connector...
python register_connector.py

REM --- 8) Register JSON Schemas + load ksqlDB streams/tables ------------------
echo [8/9] Waiting for Schema Registry on http://localhost:8081 ...
set /a count=0
:wait_sr
curl -fsS http://127.0.0.1:8081/subjects >nul 2>&1
if %errorlevel% equ 0 goto sr_ready
set /a count+=1
if %count% geq 30 goto sr_skip
timeout /t 2 /nobreak >nul
goto wait_sr
:sr_skip
echo       WARNING: Schema Registry did not become ready in time, skipping.
goto sr_done
:sr_ready
echo       Registering JSON Schemas (taxi-trips, gps-pings, surge-events, ...) ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0assets\register_schemas.ps1"
:sr_done

echo       Waiting for ksqlDB on http://localhost:8088 ...
set /a count=0
:wait_ksql
curl -fsS http://127.0.0.1:8088/info >nul 2>&1
if %errorlevel% equ 0 goto ksql_ready
set /a count+=1
if %count% geq 40 goto ksql_skip
timeout /t 3 /nobreak >nul
goto wait_ksql
:ksql_skip
echo       WARNING: ksqlDB did not become ready in time, skipping ksql_taxi.sql.
goto ksql_done
:ksql_ready
echo       Loading ksql_taxi.sql (streams + windowed tables) ...
docker cp "%~dp0ksql_taxi.sql" ksqldb-cli:/tmp/ksql_taxi.sql >nul
docker exec ksqldb-cli bash -lc "sed -n '1,/Pull query examples/p' /tmp/ksql_taxi.sql > /tmp/ksql_run.sql"
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --file /tmp/ksql_run.sql >nul 2>&1
echo       ksqlDB objects:
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW STREAMS; SHOW TABLES;" 2>nul | findstr /R "TRIPS_RAW SURGE_TRIPS REVENUE DRIVER_SHIFTS"
:ksql_done

REM --- 9) Optional: launch 6 pipeline panes in Windows Terminal ---------------
if "%LAUNCH_PANES%"=="1" (
    where wt.exe >nul 2>&1
    if errorlevel 1 (
        echo [9/9] Windows Terminal ^(wt.exe^) not found. Skipping pane launch.
    ) else (
        echo [9/9] Launching 6 pipeline panes in Windows Terminal...
        powershell -ExecutionPolicy Bypass -File "%~dp0start_pipeline.ps1"
    )
) else (
    echo [9/9] Skipping pane launch ^(re-run with: bootstrap.cmd --launch-panes^).
)

REM --- Print URLs -------------------------------------------------------------
echo.
echo =================================================================
echo   STACK IS UP. Open these in your browser:
echo =================================================================
echo   Live Taxi Dashboard : http://localhost:5000
echo   Kafka UI            : http://localhost:8080
echo   Grafana             : http://localhost:3000   (admin/admin)
echo   Prometheus          : http://localhost:9090
echo   Alertmanager        : http://localhost:9093
echo   Kafka Connect REST  : http://localhost:8083
echo   Schema Registry     : http://localhost:8081
echo   Postgres            : localhost:5432  (taxi/taxi/taxi)
echo   ksqlDB CLI          : docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
echo =================================================================
echo.
echo To launch all 6 pipeline services in one Windows Terminal window:
echo   bootstrap.cmd --launch-panes
echo or directly:
echo   powershell -ExecutionPolicy Bypass -File start_pipeline.ps1
echo.

:end
endlocal
