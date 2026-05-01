# Stop + uninstall Firebird 2.5 Windows service. Local-dev cleanup helper.
# CI runners are ephemeral so this is not invoked from workflows.
param(
    [string]$FBDir = "C:\firebird"
)

$ErrorActionPreference = "Continue"

$instsvc = Join-Path $FBDir "bin\instsvc.exe"
if (-not (Test-Path $instsvc)) {
    Write-Host "instsvc.exe not found at $instsvc - nothing to stop"
    return
}

Push-Location (Join-Path $FBDir "bin")
try {
    & $instsvc stop 2>&1 | Out-Host
    & $instsvc remove 2>&1 | Out-Host
} finally {
    Pop-Location
}

Write-Host "fb25_stopped"
