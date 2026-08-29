# 仓库根目录：git pull → sidecar + 前端 + Rust release → 前台启动。
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Test-Path "config\default.yaml")) {
    Write-Error "run from the repo (missing config/default.yaml)"
}

Write-Host "== stop running dex-arbitr =="
Get-Process dex-arbitr -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

Write-Host "== git pull =="
git pull
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "== sidecar =="
Push-Location scripts\exchange_sidecar
try {
    go build -o exchange_sidecar.exe .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host "== web =="
Push-Location web
try {
    npm install
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    npm run build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host "== rust release =="
cargo build --release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "== start =="
Write-Host "panel: http://127.0.0.1:8090/   stop: Ctrl+C"
if (-not $env:RUST_LOG) { $env:RUST_LOG = "info" }
& .\target\release\dex-arbitr.exe
exit $LASTEXITCODE
