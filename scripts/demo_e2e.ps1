$ErrorActionPreference = "Stop"

Write-Host "[ops-commander] phase6 end-to-end demo starting"

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

docker compose up -d

Write-Host "[ops-commander] launch services in separate terminals:"
Write-Host "A: python -m apps.ingestor.main"
Write-Host "B: python -m apps.simulator.main"
Write-Host "C: python -m apps.correlator.main"
Write-Host "D: python -m apps.causal_engine.main"
Write-Host "E: python -m apps.reporter_api.main"
Write-Host "F: python -m apps.replay_worker.main"

Write-Host "[ops-commander] wait 90 seconds for incident formation"
Start-Sleep -Seconds 90

Write-Host "[ops-commander] fetch latest incidents"
try {
    $incidents = Invoke-RestMethod -Method Get -Uri "http://localhost:8000/incidents?page=1&page_size=5"
    $incidents | ConvertTo-Json -Depth 5

    if ($incidents.items.Count -gt 0) {
        $incidentId = $incidents.items[0].incident_id
        Write-Host "[ops-commander] fetch incident report for $incidentId"
        Invoke-RestMethod -Method Get -Uri "http://localhost:8000/incidents/$incidentId/report" | ConvertTo-Json -Depth 8

        Write-Host "[ops-commander] enqueue replay job"
        $body = @{ incident_id = $incidentId; speed_factor = 5.0 } | ConvertTo-Json
        Invoke-RestMethod -Method Post -Uri "http://localhost:8000/replay/jobs" -Body $body -ContentType "application/json" | ConvertTo-Json -Depth 5
    }
}
catch {
    Write-Warning "[ops-commander] API check failed: $($_.Exception.Message)"
}

Write-Host "[ops-commander] demo completed"
