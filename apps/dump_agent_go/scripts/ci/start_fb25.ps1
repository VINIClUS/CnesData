# Install + start Firebird 2.5 SuperServer as Windows service for CI.
# Required because GitHub Actions kills detached fbserver -a processes
# between workflow steps; only services persist.
#
# Local-dev WARNING: this installs a system-wide Windows service.
# Pair with stop_fb25.ps1 for cleanup. CI runners are ephemeral so no
# uninstall needed.
param(
    [string]$FBDir = "C:\firebird",
    [int]$Port = 3050,
    [int]$ReadyTimeoutSec = 30
)

$ErrorActionPreference = "Stop"

$instreg = Join-Path $FBDir "bin\instreg.exe"
$instsvc = Join-Path $FBDir "bin\instsvc.exe"

if (-not (Test-Path $instreg)) { throw "instreg.exe not found at $instreg - run setup_fb25.ps1 first" }
if (-not (Test-Path $instsvc)) { throw "instsvc.exe not found at $instsvc - run setup_fb25.ps1 first" }

$env:FIREBIRD      = $FBDir
$env:FIREBIRD_LOCK = $FBDir

Push-Location (Join-Path $FBDir "bin")
try {
    Write-Host "instreg install..."
    & $instreg install -z 2>&1 | Out-Host

    Write-Host "instsvc install (SuperServer + guardian, auto-start)..."
    & $instsvc install -auto -superserver -guardian -z 2>&1 | Out-Host

    Write-Host "instsvc start..."
    & $instsvc start 2>&1 | Out-Host
} finally {
    Pop-Location
}

$deadline = (Get-Date).AddSeconds($ReadyTimeoutSec)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 1
    $listen = Test-NetConnection -ComputerName localhost -Port $Port `
        -InformationLevel Quiet -WarningAction SilentlyContinue
    if ($listen) {
        Write-Host "fb25_started port=$Port service=FirebirdServerDefaultInstance dir=$FBDir"
        return
    }
}

throw "FB 2.5 service not listening on port $Port within ${ReadyTimeoutSec}s"
