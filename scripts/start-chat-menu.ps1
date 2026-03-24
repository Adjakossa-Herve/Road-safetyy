$ErrorActionPreference = "Stop"

$composeFile = ".devcontainer/docker-compose-postgres.yml"
$maxWaitSeconds = 30

Write-Host "Starting PostgreSQL container..."
docker compose -f $composeFile up -d | Out-Host

Write-Host "Waiting for PostgreSQL on localhost:5888..."
$ready = $false
for ($i = 1; $i -le $maxWaitSeconds; $i++) {
    $test = Test-NetConnection localhost -Port 5888 -WarningAction SilentlyContinue
    if ($test.TcpTestSucceeded) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 1
}

if (-not $ready) {
    throw "PostgreSQL is not reachable on localhost:5888 after $maxWaitSeconds seconds."
}

$env:ROAD_SAFETY_MODE = "menu"
Write-Host "Launching road-safety chat in menu mode..."
poetry run road-safety chat
