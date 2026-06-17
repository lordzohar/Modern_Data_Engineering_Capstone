#Requires -Version 5.1
# Registers JSON Schemas in the Confluent Schema Registry so the
# "Schema Registry" tab in Kafka UI is populated for screenshots.
# These schemas describe the messages produced by taxi_simulator.py /
# taxi_consumer.py — they are *advisory* (the producers use JsonConverter
# without auto-registration), so registering them here is purely
# documentation-as-code.

$ErrorActionPreference = 'Stop'
$sr = 'http://127.0.0.1:8081'

function Register-Schema {
    param([string]$Subject, [string]$SchemaJson)
    $body = @{ schemaType = 'JSON'; schema = $SchemaJson } | ConvertTo-Json -Compress
    Write-Host "Registering $Subject ..."
    $resp = Invoke-RestMethod -Method Post `
        -Uri "$sr/subjects/$Subject/versions" `
        -ContentType 'application/vnd.schemaregistry.v1+json' `
        -Body $body
    Write-Host "  -> id=$($resp.id)"
}

$tripSchema = @'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TaxiTrip",
  "type": "object",
  "required": ["trip_id","driver_id","pickup_zone","dropoff_zone","total_amount"],
  "properties": {
    "trip_id":          {"type": "string"},
    "driver_id":        {"type": "string"},
    "pickup_zone":      {"type": "string"},
    "dropoff_zone":     {"type": "string"},
    "distance_miles":   {"type": "number"},
    "duration_minutes": {"type": "number"},
    "passenger_count":  {"type": "integer"},
    "payment_type":     {"type": "string", "enum": ["cash","card","wallet"]},
    "fare_amount":      {"type": "number"},
    "tip_amount":       {"type": "number"},
    "total_amount":     {"type": "number"},
    "surge_multiplier": {"type": "number"},
    "pickup_time":      {"type": "string", "format": "date-time"}
  }
}
'@

$gpsSchema = @'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "GpsPing",
  "type": "object",
  "required": ["driver_id","lat","lon","ts"],
  "properties": {
    "driver_id": {"type": "string"},
    "lat":       {"type": "number"},
    "lon":       {"type": "number"},
    "speed_mph": {"type": "number"},
    "heading":   {"type": "number"},
    "ts":        {"type": "string", "format": "date-time"}
  }
}
'@

$surgeSchema = @'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "SurgeEvent",
  "type": "object",
  "required": ["zone","multiplier","window_start","window_end"],
  "properties": {
    "zone":         {"type": "string"},
    "multiplier":   {"type": "number"},
    "trip_count":   {"type": "integer"},
    "revenue":      {"type": "number"},
    "window_start": {"type": "string", "format": "date-time"},
    "window_end":   {"type": "string", "format": "date-time"}
  }
}
'@

$dlqSchema = @'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DeadLetterRecord",
  "type": "object",
  "required": ["original_topic","reason","payload"],
  "properties": {
    "original_topic": {"type": "string"},
    "reason":         {"type": "string"},
    "error":          {"type": "string"},
    "payload":        {"type": "string"},
    "ts":             {"type": "string", "format": "date-time"}
  }
}
'@

Register-Schema -Subject 'taxi-trips-value'    -SchemaJson $tripSchema
Register-Schema -Subject 'gps-pings-value'     -SchemaJson $gpsSchema
Register-Schema -Subject 'trips-clean-value'   -SchemaJson $tripSchema
Register-Schema -Subject 'trips-enriched-value' -SchemaJson $tripSchema
Register-Schema -Subject 'surge-events-value'  -SchemaJson $surgeSchema
Register-Schema -Subject 'trips-dlq-value'     -SchemaJson $dlqSchema

Write-Host ''
Write-Host 'Subjects now in registry:'
(Invoke-RestMethod "$sr/subjects") | ForEach-Object { Write-Host "  - $_" }
