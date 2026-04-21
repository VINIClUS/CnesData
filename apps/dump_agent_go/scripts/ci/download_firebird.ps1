# Download Firebird 2.5 portable + fbclient.dll para C:\firebird
# Uses direct SF mirror URL (bypasses /download interstitial HTML page).
# Retries on transient failures.
$ErrorActionPreference = "Stop"
# Direct mirror URL — bypasses the SF /download redirect page that
# Invoke-WebRequest returns as ~200KB HTML instead of the actual zip.
$url = "https://downloads.sourceforge.net/project/firebird/firebird-win32/2.5.9-Release/Firebird-2.5.9.27139-0_x64.zip"
$dest = "C:\firebird.zip"
$extractPath = "C:\firebird"
$maxAttempts = 4
$minSize = 5000000  # FB 2.5 zip is ~12MB; anything under 5MB is junk

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    try {
        if (Test-Path $dest) { Remove-Item $dest -Force }
        Write-Host "Downloading Firebird 2.5 (attempt $attempt/$maxAttempts)..."
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing `
            -UserAgent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CnesData-CI" `
            -MaximumRedirection 10 `
            -TimeoutSec 180

        $size = (Get-Item $dest).Length
        if ($size -lt $minSize) {
            throw "downloaded file too small ($size bytes; expected > $minSize), likely truncated or HTML interstitial"
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
