# Extract Firebird 2.5.9 portable from LFS fixture to a target directory.
# Replaces download_firebird.ps1 (retired 2026-04 - see issue #52).
# No network access required.
param(
    [string]$ExtractPath = "C:\firebird"
)

$ErrorActionPreference = "Stop"

$repoRoot    = Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..")
$lfsZip      = Join-Path $repoRoot "docs\fixtures\firebird\Firebird-2.5.9.27139-0_x64.zip"
$expectedSha = "707e05bae8994b06cec60815a292078db82d8e75616f4dd514b7e417a3ee2137"
$minSize     = 5000000

if (-not (Test-Path $lfsZip)) {
    throw "FB 2.5 LFS fixture missing at $lfsZip - run 'git lfs pull'"
}

$size = (Get-Item $lfsZip).Length
if ($size -lt $minSize) {
    throw "FB 2.5 zip size $size bytes - LFS pointer not hydrated; run 'git lfs pull'"
}

$actualSha = (Get-FileHash $lfsZip -Algorithm SHA256).Hash.ToLower()
if ($actualSha -ne $expectedSha) {
    throw "FB 2.5 zip SHA256 mismatch expected=$expectedSha got=$actualSha"
}

if (Test-Path $ExtractPath) {
    Remove-Item $ExtractPath -Recurse -Force
}

Write-Host "Extracting FB 2.5 from LFS (size=$size bytes) to $ExtractPath..."
Expand-Archive -Path $lfsZip -DestinationPath $ExtractPath -Force

if (-not (Test-Path "$ExtractPath\bin\fbclient.dll")) {
    throw "fbclient.dll missing after extract; archive layout changed"
}

Write-Host "fb25_ready path=$ExtractPath fbclient=$ExtractPath\bin\fbclient.dll"
