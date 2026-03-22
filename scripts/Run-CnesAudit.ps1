<#
.SYNOPSIS
    Execucao automatizada do pipeline de auditoria CnesData.

.DESCRIPTION
    Ativa o ambiente virtual Python, calcula a competencia da base DATASUS
    (mes atual - N meses de atraso), executa o pipeline e rotaciona logs.
    Projetado para execucao via Windows Task Scheduler.

.PARAMETER Competencia
    Competencia no formato YYYY-MM. Se omitido, calcula automaticamente.

.PARAMETER DelayMeses
    Meses de atraso da publicacao DATASUS. Padrao: 2.

.PARAMETER SkipNacional
    Pular cross-check com BigQuery (modo offline).

.PARAMETER SkipHr
    Pular cross-check com folha de RH.

.PARAMETER OutputDir
    Diretorio de saida. Padrao: data/processed.

.PARAMETER MaxLogs
    Quantidade de arquivos de log a manter. Padrao: 6.

.PARAMETER SmtpServer
    Servidor SMTP para notificacao de erro (opcional).

.PARAMETER EmailTo
    Endereco de email para notificacao de erro (opcional).

.PARAMETER EmailFrom
    Endereco remetente para notificacao (opcional).

.EXAMPLE
    .\scripts\Run-CnesAudit.ps1 -SkipNacional -SkipHr

.EXAMPLE
    .\scripts\Run-CnesAudit.ps1 -Competencia 2024-12 -SkipNacional
#>

[CmdletBinding()]
param(
    [string]$Competencia,
    [int]$DelayMeses = 2,
    [switch]$SkipNacional,
    [switch]$SkipHr,
    [string]$OutputDir,
    [int]$MaxLogs = 6,
    [string]$SmtpServer,
    [string]$EmailTo,
    [string]$EmailFrom
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

# -- Caminhos --
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$VenvPython = Join-Path $ProjectRoot "venv\Scripts\python.exe"
$MainPy = Join-Path $ProjectRoot "src\main.py"
$LogDir = Join-Path $ProjectRoot "logs"
$TimestampFile = Join-Path $LogDir "last_run.txt"

# -- Validacao --
if (-not (Test-Path $VenvPython)) {
    Write-Error "Python do venv nao encontrado em: $VenvPython"
    exit 1
}
if (-not (Test-Path $MainPy)) {
    Write-Error "main.py nao encontrado em: $MainPy"
    exit 1
}

# -- Competencia --
if ([string]::IsNullOrEmpty($Competencia)) {
    $DataRef = (Get-Date).AddMonths(-$DelayMeses)
    $Competencia = $DataRef.ToString("yyyy-MM")
    Write-Host "[INFO] Competencia auto-detectada: $Competencia (delay=$DelayMeses meses)"
}
else {
    Write-Host "[INFO] Competencia informada: $Competencia"
}

# -- Diretorio de logs --
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$RunTimestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$RunLogFile = Join-Path $LogDir "run_${RunTimestamp}.log"

# -- Montar argumentos CLI --
$CliArgs = @($MainPy, "-c", $Competencia)

if ($SkipNacional) {
    $CliArgs += "--skip-nacional"
    Write-Host "[INFO] BigQuery cross-check: DESATIVADO"
}
if ($SkipHr) {
    $CliArgs += "--skip-hr"
    Write-Host "[INFO] HR cross-check: DESATIVADO"
}
if (-not [string]::IsNullOrEmpty($OutputDir)) {
    $CliArgs += @("-o", $OutputDir)
    Write-Host "[INFO] Output dir: $OutputDir"
}
if ($VerbosePreference -eq "Continue") {
    $CliArgs += "-v"
}

# -- Executar pipeline --
Write-Host ""
Write-Host "================================================================"
Write-Host "  CnesData Pipeline - $Competencia"
$StartTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Host "  Inicio: $StartTime"
Write-Host "================================================================"
Write-Host ""

$Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$ExitCode = 0

try {
    & $VenvPython $CliArgs 2>&1 | Tee-Object -FilePath $RunLogFile
    $ExitCode = $LASTEXITCODE
}
catch {
    Write-Error "Erro ao executar pipeline: $_"
    $ExitCode = 1
}

$Stopwatch.Stop()
$Duracao = $Stopwatch.Elapsed.ToString("hh\:mm\:ss")

# -- Resumo de execucao --
Write-Host ""
Write-Host "================================================================"
if ($ExitCode -eq 0) {
    Write-Host "  RESULTADO: SUCESSO"
}
else {
    Write-Host "  RESULTADO: FALHA (exit code $ExitCode)"
}
Write-Host "  Competencia: $Competencia"
Write-Host "  Duracao: $Duracao"
Write-Host "  Log: $RunLogFile"
Write-Host "================================================================"

# Salvar timestamp da ultima execucao
$LastRunLine = "$RunTimestamp competencia=$Competencia exit=$ExitCode duracao=$Duracao"
$LastRunLine | Out-File -FilePath $TimestampFile -Encoding UTF8

# -- Rotacao de logs --
$LogFiles = Get-ChildItem -Path $LogDir -Filter "run_*.log" | Sort-Object Name -Descending
if ($LogFiles.Count -gt $MaxLogs) {
    $Excesso = $LogFiles | Select-Object -Skip $MaxLogs
    foreach ($Arquivo in $Excesso) {
        Remove-Item $Arquivo.FullName -Force
        Write-Host "[INFO] Log removido (rotacao): $($Arquivo.Name)"
    }
    Write-Host "[INFO] Rotacao de logs: mantidos $MaxLogs, removidos $($Excesso.Count)"
}

# -- Notificacao de erro (opcional) --
if ($ExitCode -ne 0 -and -not [string]::IsNullOrEmpty($SmtpServer) -and -not [string]::IsNullOrEmpty($EmailTo)) {
    $Subject = "CnesData FALHA - Competencia $Competencia"
    $Body = "Pipeline CnesData falhou.`r`n"
    $Body += "Competencia: $Competencia`r`n"
    $Body += "Data/Hora: $RunTimestamp`r`n"
    $Body += "Exit Code: $ExitCode`r`n"
    $Body += "Duracao: $Duracao`r`n"
    $Body += "Log: $RunLogFile`r`n"
    $Body += "`r`nVerifique o log anexo para detalhes."

    if ([string]::IsNullOrEmpty($EmailFrom)) {
        $EmailFromAddr = "cnesdata@localhost"
    }
    else {
        $EmailFromAddr = $EmailFrom
    }

    try {
        Send-MailMessage `
            -From $EmailFromAddr `
            -To $EmailTo `
            -Subject $Subject `
            -Body $Body `
            -SmtpServer $SmtpServer `
            -Attachments $RunLogFile `
            -Priority High
        Write-Host "[INFO] Email de notificacao enviado para $EmailTo"
    }
    catch {
        Write-Warning "Falha ao enviar email de notificacao: $_"
    }
}

exit $ExitCode