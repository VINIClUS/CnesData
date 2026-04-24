# Start Firebird 2.5 server in foreground/application mode for CI integration.
# Mirrors start_fb15.ps1 pattern: set FIREBIRD env vars, run fbserver.exe -a
# directly (skip fbguard supervisor — CI doesn't need restart-on-crash).
param(
    [string]$FBDir = "C:\firebird",
    [int]$Port = 3050
)

$ErrorActionPreference = "Stop"

$fbserver = Join-Path $FBDir "bin\fbserver.exe"
if (-not (Test-Path $fbserver)) {
    throw "fbserver.exe not found at $fbserver - run setup_fb25.ps1 first"
}

$env:FIREBIRD      = $FBDir
$env:FIREBIRD_LOCK = $FBDir

$stdout = Join-Path $FBDir "fbserver.stdout.log"
$stderr = Join-Path $FBDir "fbserver.stderr.log"

$process = Start-Process -FilePath $fbserver `
    -ArgumentList "-a" `
    -WorkingDirectory $FBDir `
    -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr

Start-Sleep -Seconds 8

if ($process.HasExited) {
    Get-Content $stdout -ErrorAction SilentlyContinue
    Get-Content $stderr -ErrorAction SilentlyContinue
    throw "fbserver.exe exited early, code=$($process.ExitCode)"
}

Write-Host "fb25_started port=$Port pid=$($process.Id) dir=$FBDir"
