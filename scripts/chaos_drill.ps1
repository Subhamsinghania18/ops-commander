param(
    [ValidateSet("drop-messages", "restart-ingestor", "restart-correlator", "db-outage", "db-recovery")]
    [string]$Scenario = "drop-messages",
    [int]$DurationSeconds = 30
)

$ErrorActionPreference = "Stop"

function Find-ProcessByCommandLine([string]$needle) {
    Get-CimInstance Win32_Process |
        Where-Object { $_.CommandLine -and $_.CommandLine -like "*$needle*" }
}

switch ($Scenario) {
    "drop-messages" {
        Write-Host "[chaos] Set CHAOS_SIM_DROP_RATE in .env (example 0.2), restart simulator, and observe pipeline degradation."
        Write-Host "[chaos] Revert CHAOS_SIM_DROP_RATE to 0 and restart simulator for recovery."
    }
    "restart-ingestor" {
        Write-Host "[chaos] restarting ingestor process"
        $proc = Find-ProcessByCommandLine "apps.ingestor.main"
        if ($proc) { $proc | ForEach-Object { Stop-Process -Id $_.ProcessId -Force } }
        Start-Sleep -Seconds 2
        Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; python -m apps.ingestor.main"
    }
    "restart-correlator" {
        Write-Host "[chaos] restarting correlator process"
        $proc = Find-ProcessByCommandLine "apps.correlator.main"
        if ($proc) { $proc | ForEach-Object { Stop-Process -Id $_.ProcessId -Force } }
        Start-Sleep -Seconds 2
        Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; python -m apps.correlator.main"
    }
    "db-outage" {
        Write-Host "[chaos] stopping postgres container for $DurationSeconds seconds"
        docker compose stop postgres
        Start-Sleep -Seconds $DurationSeconds
        Write-Host "[chaos] postgres remains down; run db-recovery to restore"
    }
    "db-recovery" {
        Write-Host "[chaos] starting postgres container"
        docker compose start postgres
    }
}
