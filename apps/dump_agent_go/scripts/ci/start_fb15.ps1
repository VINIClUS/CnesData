# Start Firebird 1.5.6 standalone server (x86) for integration tests.
param(
    [string]$FBDir = ".cache/firebird-1.5.6-server",
    [int]$Port = 3050
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $FBDir)) {
    python scripts/fb156_setup.py --server
}

$fbserver = Join-Path $FBDir "bin\fbserver.exe"
if (-not (Test-Path $fbserver)) {
    throw "fbserver.exe not found at $fbserver"
}

$abs = (Resolve-Path $FBDir).Path
$env:FIREBIRD = $abs
$env:FIREBIRD_LOCK = $abs

$stdout = Join-Path $abs "fbserver.stdout.log"
$stderr = Join-Path $abs "fbserver.stderr.log"

$process = Start-Process -FilePath $fbserver `
    -ArgumentList "-a" `
    -WorkingDirectory $abs `
    -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr

Start-Sleep -Seconds 8

if ($process.HasExited) {
    Get-Content $stdout -ErrorAction SilentlyContinue
    Get-Content $stderr -ErrorAction SilentlyContinue
    throw "fbserver.exe exited early, code=$($process.ExitCode)"
}

Write-Host "fb15_started port=$Port pid=$($process.Id) dir=$abs"
