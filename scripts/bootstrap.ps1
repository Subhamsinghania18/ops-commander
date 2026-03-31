$ErrorActionPreference = "Stop"

Write-Host "[ops-commander] preparing local environment"

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[ops-commander] created .env from .env.example"
}

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"

docker compose up -d

Write-Host "[ops-commander] bootstrapped successfully"
