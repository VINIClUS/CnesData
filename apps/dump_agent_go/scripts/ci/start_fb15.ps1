# Start Firebird 1.5.6 standalone server (x86) for integration tests.
param(
    [string]$FBDir = ".cache/firebird-1.5.6",
    [int]$Port = 3050
)

if (-not (Test-Path $FBDir)) {
    python scripts/fb156_setup.py
}

$fbserver = Join-Path $FBDir "bin\fbserver.exe"
if (-not (Test-Path $fbserver)) {
    throw "fbserver.exe not found at $fbserver"
}

$process = Start-Process -FilePath $fbserver `
    -ArgumentList "-a", "-p", $Port `
    -WorkingDirectory $FBDir `
    -PassThru -WindowStyle Hidden

Start-Sleep -Seconds 5

Write-Host "fb15_started port=$Port pid=$($process.Id)"
