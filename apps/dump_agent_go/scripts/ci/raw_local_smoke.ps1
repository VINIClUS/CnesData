$ErrorActionPreference = "Stop"
$root = (Resolve-Path "../..").Path
$python = Join-Path $root ".venv/Scripts/python.exe"
$agent = "C:\tmp\dumpagent-raw.exe"
$env:DB_URL = "postgresql://test:test@localhost:5432/test"
$env:PROFILE = "local"
$env:TENANT_ID = "354130"
$env:DATA_DIR = "C:\tmp\raw-local"
$env:RAW_LOCAL_TOKEN = "raw-ci-token"
$env:ADMIN_TOKEN = "raw-ci-admin"
$env:AGENT_MTLS_REQUIRED = "false"
$env:AGENT_ALLOW_INSECURE = "true"
$env:AGENT_DISABLE_DISCOVER = "true"
$env:AGENT_APPDATA_DIR = "C:\tmp\raw-agent"
$env:DUMP_LOGS_DIR = "C:\tmp\raw-agent-logs"
$env:DUMP_MAX_JITTER_SECONDS = "1"
$env:MACHINE_ID = "raw-ci-agent"
$env:CENTRAL_API_URL = "http://127.0.0.1:8000"
$env:COD_MUN_IBGE = "354130"
$env:CNES_DB_PATH = "C:/tmp/CNES_test.gdb"
$env:SIHD_DB_PATH = "C:/tmp/SIHD_test.gdb"
$env:BPA_DB_PATH = "C:/tmp/BPAMAG_test.gdb"
$env:SIA_DIR = (Resolve-Path "test/integration/fixtures/sia_synthetic").Path

function Start-RawAgent([string]$stdout, [string]$stderr) {
    $options = @{
        FilePath = $agent
        ArgumentList = @("run", "--raw")
        PassThru = $true
        RedirectStandardOutput = $stdout
        RedirectStandardError = $stderr
    }
    return Start-Process @options
}

New-Item -ItemType Directory -Path $env:DATA_DIR -Force | Out-Null
go build -o $agent ./cmd/dumpagent
$api = Start-Process -FilePath $python -ArgumentList @(
    "-m", "uvicorn", "central_api.app:create_app", "--factory",
    "--host", "127.0.0.1", "--port", "8000"
) -PassThru -RedirectStandardOutput "C:\tmp\raw-api.out" -RedirectStandardError "C:\tmp\raw-api.err"
try {
    $healthy = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $null = Invoke-WebRequest "http://127.0.0.1:8000/openapi.json" -UseBasicParsing
            $healthy = $true
            break
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    if (-not $healthy) {
        Get-Content "C:\tmp\raw-api.err" -ErrorAction SilentlyContinue
        Get-Content "C:\tmp\raw-api.out" -ErrorAction SilentlyContinue
        throw "raw_api_not_ready"
    }

    $headers = @{ "X-Admin-Token" = "raw-ci-admin"; "Idempotency-Key" = "raw-ci-202601" }
    $body = @{
        tenant_id = "354130"
        agent_id = "raw-ci-agent"
        competencia = "2026-01"
    } | ConvertTo-Json
    $enqueueUri = "http://127.0.0.1:8000/api/v1/admin/raw-jobs/enqueue"
    $first = Invoke-RestMethod -Method Post -Uri $enqueueUri -Headers $headers `
        -ContentType "application/json" -Body $body
    if ($first.job_ids.Count -ne 10) { throw "raw_enqueue_count_invalid" }

    $runningAgent = Start-RawAgent "C:\tmp\raw-agent.out" "C:\tmp\raw-agent.err"
    try {
        $statusScript = Join-Path $PWD "scripts/ci/raw_status.py"
        $stateDb = Join-Path $env:DATA_DIR "state/cnesdata.sqlite3"
        $complete = $false
        for ($i = 0; $i -lt 180; $i++) {
            $count = & $python $statusScript $stateDb
            if ($count -eq "10") { $complete = $true; break }
            if ($runningAgent.HasExited) { break }
            Start-Sleep -Seconds 1
        }
        if (-not $complete) {
            Write-Host "raw_smoke_status succeeded=$count agent_exited=$($runningAgent.HasExited)"
            Get-Content "C:\tmp\raw-agent.err" -Tail 100 -ErrorAction SilentlyContinue
            Get-Content "C:\tmp\raw-agent.out" -Tail 100 -ErrorAction SilentlyContinue
            $logFile = Join-Path $env:DUMP_LOGS_DIR "dumpagent.log"
            Get-Content $logFile -Tail 100 -ErrorAction SilentlyContinue
            throw "raw_jobs_not_complete"
        }
    } finally {
        Stop-Process -Id $runningAgent.Id -Force -ErrorAction SilentlyContinue
    }

    $replay = Invoke-RestMethod -Method Post -Uri $enqueueUri -Headers $headers `
        -ContentType "application/json" -Body $body
    if (@(Compare-Object $first.job_ids $replay.job_ids).Count -ne 0) {
        throw "raw_enqueue_replay_mismatch"
    }
    $restarted = Start-RawAgent "C:\tmp\raw-agent-restart.out" "C:\tmp\raw-agent-restart.err"
    Start-Sleep -Seconds 10
    if ($restarted.HasExited -and $restarted.ExitCode -ne 0) {
        Get-Content "C:\tmp\raw-agent-restart.err" -Tail 100 -ErrorAction SilentlyContinue
        throw "raw_restart_failed"
    }
    Stop-Process -Id $restarted.Id -Force -ErrorAction SilentlyContinue
    Write-Host "raw_smoke_completed jobs=10 replay=ok restart=ok"
} finally {
    Stop-Process -Id $api.Id -Force -ErrorAction SilentlyContinue
}
