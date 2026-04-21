# Download Firebird 2.5 portable + fbclient.dll para C:\firebird
# Retries on transient SourceForge failures (partial/corrupt download).
$ErrorActionPreference = "Stop"
$url = "https://sourceforge.net/projects/firebird/files/firebird-win32/2.5.9-Release/Firebird-2.5.9.27139-0_x64.zip/download"
$dest = "C:\firebird.zip"
$extractPath = "C:\firebird"
$maxAttempts = 4

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    try {
        if (Test-Path $dest) { Remove-Item $dest -Force }
        Write-Host "Downloading Firebird 2.5 (attempt $attempt/$maxAttempts)..."
        Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing -TimeoutSec 120

        $size = (Get-Item $dest).Length
        if ($size -lt 1000000) {
            throw "downloaded file too small ($size bytes), likely truncated"
        }

        Write-Host "Extracting (size=$size bytes)..."
        if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }
        Expand-Archive -Path $dest -DestinationPath $extractPath -Force

        Write-Host "Firebird extracted at $extractPath"
        Write-Host "fbclient.dll at $extractPath\bin\fbclient.dll"
        exit 0
    } catch {
        Write-Host "attempt_failed attempt=$attempt error=$($_.Exception.Message)"
        if ($attempt -lt $maxAttempts) {
            $sleep = [Math]::Min(30, [Math]::Pow(2, $attempt) * 5)
            Write-Host "retrying in $sleep seconds..."
            Start-Sleep -Seconds $sleep
        } else {
            Write-Host "all_attempts_failed"
            throw
        }
    }
}
