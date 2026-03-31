param(
    [string]$ApiBase = "http://localhost:8000",
    [int]$WaitSeconds = 20
)

$ErrorActionPreference = "Stop"

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$outDir = Join-Path "artifacts/showcase" $stamp
New-Item -ItemType Directory -Path $outDir -Force | Out-Null

Write-Host "[showcase] waiting $WaitSeconds seconds for pipeline stabilization"
Start-Sleep -Seconds $WaitSeconds

function Save-Json($name, $obj) {
    $path = Join-Path $outDir $name
    ($obj | ConvertTo-Json -Depth 20) | Out-File -FilePath $path -Encoding utf8
    return $path
}

$health = Invoke-RestMethod -Method Get -Uri "$ApiBase/health"
$ready = Invoke-RestMethod -Method Get -Uri "$ApiBase/ops/readiness"
$metrics = Invoke-RestMethod -Method Get -Uri "$ApiBase/ops/metrics"
$inc = Invoke-RestMethod -Method Get -Uri "$ApiBase/incidents?page=1&page_size=25"

$healthPath = Save-Json "health.json" $health
$readyPath = Save-Json "readiness.json" $ready
$metricsPath = Save-Json "ops_metrics.json" $metrics
$incPath = Save-Json "incidents.json" $inc

$reportPath = $null
$detailPath = $null
$replaySubmitPath = $null
$incidentId = "none"
$selectedServiceCount = 0

if ($inc.items.Count -gt 0) {
    $selected = $inc.items |
        Sort-Object `
            @{ Expression = { [int]$_.affected_services.Count }; Descending = $true }, `
            @{ Expression = { if ($_.severity -eq "critical") { 3 } elseif ($_.severity -eq "high") { 2 } elseif ($_.severity -eq "medium") { 1 } else { 0 } }; Descending = $true }, `
            @{ Expression = { [double]$_.confidence }; Descending = $true } |
        Select-Object -First 1

    $incidentId = $selected.incident_id
    $detail = Invoke-RestMethod -Method Get -Uri "$ApiBase/incidents/$incidentId"
    $report = Invoke-RestMethod -Method Get -Uri "$ApiBase/incidents/$incidentId/report"
    $selectedServiceCount = $detail.affected_services.Count

    $detailPath = Save-Json "incident_$incidentId.json" $detail
    $reportPath = Save-Json "report_$incidentId.json" $report

    $body = @{ incident_id = $incidentId; speed_factor = 5.0; note = "showcase-capture" } | ConvertTo-Json
    $replayJob = Invoke-RestMethod -Method Post -Uri "$ApiBase/replay/jobs" -Body $body -ContentType "application/json"
    $replaySubmitPath = Save-Json "replay_job_submit.json" $replayJob
}

$latestEval = Get-ChildItem "data/archive/evaluations" -Filter "*.json" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

$evalRef = $null
if ($latestEval) {
    Copy-Item $latestEval.FullName (Join-Path $outDir "latest_evaluation.json") -Force
    $evalRef = "latest_evaluation.json"
}

$summaryPath = Join-Path $outDir "SHOWCASE_SUMMARY.md"
$lines = @(
    "# Ops Commander Showcase Bundle",
    "",
    "Generated at: $stamp",
    "",
    "## Captured Files",
    "- health.json",
    "- readiness.json",
    "- ops_metrics.json",
    "- incidents.json"
)
if ($detailPath) { $lines += "- $(Split-Path $detailPath -Leaf)" }
if ($reportPath) { $lines += "- $(Split-Path $reportPath -Leaf)" }
if ($replaySubmitPath) { $lines += "- replay_job_submit.json" }
if ($evalRef) { $lines += "- $evalRef" }

$lines += ""
$lines += "## Key Signals"
$lines += "- Incident list size: $($inc.items.Count)"
$lines += "- Selected showcase incident: $incidentId"
$lines += "- Selected incident affected services: $selectedServiceCount"
$lines += "- API readiness status: $($ready.status)"
$lines += "- Service metrics snapshot captured"
$lines += ""
$lines += "Use this folder directly in demos/interviews as evidence artifacts."

$lines | Out-File -FilePath $summaryPath -Encoding utf8

Write-Host "[showcase] bundle created at $outDir"
