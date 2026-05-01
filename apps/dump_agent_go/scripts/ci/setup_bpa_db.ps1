$ErrorActionPreference = "Stop"
$fbBin = "C:\firebird\bin"
$dbPath = "C:\tmp\BPAMAG_test.gdb"
$sqlFixture = ".\test\integration\fixtures\BPA_synthetic.sql"

New-Item -ItemType Directory -Path "C:\tmp" -Force | Out-Null

Write-Host "Creating $dbPath..."
& "$fbBin\isql.exe" -u SYSDBA -p masterkey -ch UTF8 -q -i $sqlFixture -o create_bpa_db.log

Write-Host "Verifying..."
if (Test-Path $dbPath) {
    Write-Host "BPA database created successfully at $dbPath"
    exit 0
} else {
    Write-Host "BPA database creation FAILED"
    Get-Content create_bpa_db.log
    exit 1
}
