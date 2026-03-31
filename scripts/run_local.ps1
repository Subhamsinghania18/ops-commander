$ErrorActionPreference = "Stop"

Write-Host "[ops-commander] starting local stack"
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

docker compose up -d

Write-Host "[ops-commander] start ingestor in terminal A:"
Write-Host "python -m apps.ingestor.main"
Write-Host "[ops-commander] start simulator in terminal B:"
Write-Host "python -m apps.simulator.main"
Write-Host "[ops-commander] start correlator in terminal C:"
Write-Host "python -m apps.correlator.main"
Write-Host "[ops-commander] start causal engine in terminal D:"
Write-Host "python -m apps.causal_engine.main"
Write-Host "[ops-commander] start reporter api in terminal E:"
Write-Host "python -m apps.reporter_api.main"
Write-Host "[ops-commander] start replay worker in terminal F:"
Write-Host "python -m apps.replay_worker.main"
Write-Host "[ops-commander] optional full demo script: ./scripts/demo_e2e.ps1"
Write-Host "[ops-commander] optional chaos drill script: ./scripts/chaos_drill.ps1 -Scenario restart-ingestor"
