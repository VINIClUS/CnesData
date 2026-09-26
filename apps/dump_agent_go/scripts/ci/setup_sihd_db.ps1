$ErrorActionPreference = "Stop"
$fbBin = "C:\firebird\bin"
$dbPath = "C:\tmp\SIHD_test.gdb"
$sqlFixture = ".\test\integration\fixtures\SIHD_synthetic.sql"

New-Item -ItemType Directory -Path "C:\tmp" -Force | Out-Null
& "$fbBin\isql.exe" -u SYSDBA -p masterkey -ch UTF8 -q -i $sqlFixture -o create_sihd.log
if (-not (Test-Path $dbPath)) {
    Get-Content create_sihd.log
    throw "sihd_fixture_missing"
}
