# Download Firebird 2.5 portable + fbclient.dll para C:\firebird
$ErrorActionPreference = "Stop"
$url = "https://sourceforge.net/projects/firebird/files/firebird-win32/2.5.9-Release/Firebird-2.5.9.27139-0_x64.zip/download"
$dest = "C:\firebird.zip"
$extractPath = "C:\firebird"

Write-Host "Downloading Firebird 2.5..."
Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing

Write-Host "Extracting..."
Expand-Archive -Path $dest -DestinationPath $extractPath -Force

Write-Host "Firebird extracted at $extractPath"
Write-Host "fbclient.dll at $extractPath\bin\fbclient.dll"
