<#
.SYNOPSIS
    Registra o pipeline CnesData no Agendador de Tarefas do Windows.

.DESCRIPTION
    Cria uma tarefa agendada que executa Run-CnesAudit.ps1 no dia 15 de
    cada mês às 07:00 (horário em que a estação já está ligada e a rede
    disponível, antes do expediente).

.PARAMETER TaskName
    Nome da tarefa no Agendador. Padrão: "CnesData - Auditoria Mensal".

.PARAMETER DayOfMonth
    Dia do mês para execução. Padrão: 15.

.PARAMETER Time
    Hora de execução. Padrão: "07:00".

.PARAMETER User
    Usuário do Windows que executará a tarefa. Padrão: usuário atual.

.EXAMPLE
    # Registrar com valores padrão (requer Admin)
    .\scripts\Schedule-CnesAudit.ps1

.EXAMPLE
    # Customizar dia e hora
    .\scripts\Schedule-CnesAudit.ps1 -DayOfMonth 10 -Time "06:30"
#>

[CmdletBinding()]
param(
    [string]$TaskName   = "CnesData - Auditoria Mensal",
    [int]   $DayOfMonth = 15,
    [string]$Time       = "07:00",
    [string]$User       = $env:USERNAME
)

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunScript  = Join-Path $ScriptDir "Run-CnesAudit.ps1"

if (-not (Test-Path $RunScript)) {
    Write-Error "Run-CnesAudit.ps1 nao encontrado em: $RunScript"
    exit 1
}

$Action = New-ScheduledTaskAction `
    -Execute      "powershell.exe" `
    -Argument     "-ExecutionPolicy Bypass -NoProfile -File `"$RunScript`"" `
    -WorkingDirectory (Split-Path -Parent $ScriptDir)

$Trigger = New-ScheduledTaskTrigger `
    -Monthly `
    -DaysOfMonth $DayOfMonth `
    -At $Time

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

Register-ScheduledTask `
    -TaskName   $TaskName `
    -Action     $Action `
    -Trigger    $Trigger `
    -Settings   $Settings `
    -User       $User `
    -RunLevel   Limited `
    -Description "Executa auditoria mensal CNES para Presidente Epitacio/SP. Competencia auto-detectada (mes atual - 2)."

Write-Host ""
Write-Host "Tarefa agendada com sucesso!"
Write-Host "  Nome: $TaskName"
Write-Host "  Dia: $DayOfMonth de cada mes"
Write-Host "  Hora: $Time"
Write-Host "  Script: $RunScript"
Write-Host ""
Write-Host "Para verificar: Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "Para remover:   Unregister-ScheduledTask -TaskName '$TaskName'"
