# Download Firebird 2.5 portable + fbclient.dll para C:\firebird
# Uses curl.exe (native on Windows runners) — SourceForge blocks
# Invoke-WebRequest UA. Retries on transient failures.
$ErrorActionPreference = "Stop"
$url = "https://downloads.sourceforge.net/project/firebird/firebird-win32/2.5.9-Release/Firebird-2.5.9.27139-0_x64.zip"
$dest = "C:\firebird.zip"
$extractPath = "C:\firebird"
$maxAttempts = 4
$minSize = 5000000

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    try {
        if (Test-Path $dest) { Remove-Item $dest -Force }
        Write-Host "Downloading Firebird 2.5 via curl.exe (attempt $attempt/$maxAttempts)..."

        $curlExit = (Start-Process -FilePath "curl.exe" `
            -ArgumentList @(
                "-L", "-f", "-o", $dest,
                "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "--max-time", "180",
                "--retry", "2",
                "--retry-delay", "5",
                $url
            ) `
            -NoNewWindow -Wait -PassThru).ExitCode

        if ($curlExit -ne 0) {
            throw "curl.exe exit $curlExit"
        }

        $size = (Get-Item $dest).Length
        if ($size -lt $minSize) {
            throw "downloaded file too small ($size bytes; expected > $minSize)"
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
