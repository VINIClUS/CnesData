$ErrorActionPreference = "Stop"
$fbBin = "C:\firebird\bin"
$dbPath = "C:\tmp\CNES_test.gdb"
$sqlFixture = ".\test\integration\fixtures\CNES_synthetic.sql"

New-Item -ItemType Directory -Path "C:\tmp" -Force | Out-Null

Write-Host "Creating $dbPath..."
& "$fbBin\isql.exe" -u SYSDBA -p masterkey -q -i $sqlFixture -o create_db.log

Write-Host "Verifying..."
if (Test-Path $dbPath) {
    Write-Host "Database created successfully at $dbPath"
    exit 0
} else {
    Write-Host "Database creation FAILED"
    Get-Content create_db.log
    exit 1
}
