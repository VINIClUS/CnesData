# Snapshot de estado da maquina para auditoria de residuo do dumpagent.
# Uso: powershell -NoProfile -NonInteractive -EncodedCommand <b64-utf16le desse script>
# Saida: um unico JSON em stdout.

$ErrorActionPreference = 'SilentlyContinue'

function Get-FsIndexDeep($paths, $depth) {
    $out = @()
    foreach ($p in $paths) {
        if (-not (Test-Path $p)) { continue }
        Get-ChildItem -Path $p -Recurse -Depth $depth -Force -ErrorAction SilentlyContinue |
            ForEach-Object {
                $out += [PSCustomObject]@{
                    Path = $_.FullName
                    Length = if ($_.PSIsContainer) { $null } else { $_.Length }
                    LastWriteTimeUtc = $_.LastWriteTimeUtc.ToString('o')
                    IsDir = $_.PSIsContainer
                }
            }
    }
    return $out
}

function Get-FsIndexShallow($paths) {
    # Top-level children only, cheap early-warning for new/removed entries
    # in directories too large to index deeply (browser caches, etc).
    $out = @()
    foreach ($p in $paths) {
        if (-not (Test-Path $p)) { continue }
        Get-ChildItem -Path $p -Force -ErrorAction SilentlyContinue |
            ForEach-Object {
                $out += [PSCustomObject]@{
                    Path = $_.FullName
                    LastWriteTimeUtc = $_.LastWriteTimeUtc.ToString('o')
                    IsDir = $_.PSIsContainer
                }
            }
    }
    return $out
}

function Get-RegSubkeys($path) {
    if (-not (Test-Path $path)) { return @() }
    return (Get-ChildItem -Path $path -ErrorAction SilentlyContinue | Select-Object -ExpandProperty PSChildName)
}

# Broad roots: shallow only (these can contain 10k+ unrelated files - browser
# cache, Windows Update). Any new/removed top-level entry here is a lead to
# follow up on manually; the deep index below covers known agent paths.
$shallowRoots = @(
    $env:TEMP,
    $env:LOCALAPPDATA,
    $env:APPDATA,
    'C:\Windows\System32\config\systemprofile\AppData\Local',
    'C:\Windows\System32\config\systemprofile\AppData\Roaming',
    'C:\Program Files',
    'C:\Program Files (x86)',
    'C:\ProgramData'
)

# Known/candidate agent paths: full recursive index. Covers current default
# (%LOCALAPPDATA%\CnesAgent), the H5-fix candidate (C:\ProgramData\CnesAgent),
# the install dir, and this session's own scratch root.
$deepPaths = @(
    'C:\Program Files\CnesAgent',
    'C:\Program Files (x86)\CnesAgent',
    'C:\ProgramData\CnesAgent',
    "$env:LOCALAPPDATA\CnesAgent",
    "$env:APPDATA\CnesAgent",
    'C:\Windows\System32\config\systemprofile\AppData\Local\CnesAgent',
    'C:\CnesDataTest'
)

$snapshot = [ordered]@{
    TimestampUtc = (Get-Date).ToUniversalTime().ToString('o')
    Services = @(Get-Service | Select-Object Name, Status, StartType)
    ScheduledTasks = @(Get-ScheduledTask | Select-Object TaskName, State, TaskPath)
    LocalUsers = @(Get-LocalUser | Select-Object Name, Enabled)
    LocalAdmins = @(Get-LocalGroupMember -Group 'Administrators' | Select-Object Name)
    FirewallRules = @(Get-NetFirewallRule | Select-Object Name, DisplayName, Enabled)
    RegServicesSubkeys = Get-RegSubkeys 'HKLM:\SYSTEM\CurrentControlSet\Services'
    RegSoftwareSubkeys = Get-RegSubkeys 'HKLM:\SOFTWARE'
    RegSoftwareWow6432Subkeys = Get-RegSubkeys 'HKLM:\SOFTWARE\WOW6432Node'
    RegUninstall64 = Get-RegSubkeys 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall'
    RegUninstall32 = Get-RegSubkeys 'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall'
    RegEventLogApplicationSources = Get-RegSubkeys 'HKLM:\SYSTEM\CurrentControlSet\Services\EventLog\Application'
    RegRunHKLM = Get-RegSubkeys 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Run'
    RegRunOnceHKLM = Get-RegSubkeys 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce'
    RegRunHKCU = Get-RegSubkeys 'HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Run'
    RegHKCUSoftwareSubkeys = Get-RegSubkeys 'HKCU:\SOFTWARE'
    EnvMachine = [Environment]::GetEnvironmentVariables('Machine')
    EnvUser = [Environment]::GetEnvironmentVariables('User')
    FsShallow = Get-FsIndexShallow $shallowRoots
    FsDeep = Get-FsIndexDeep $deepPaths 10
}

$snapshot | ConvertTo-Json -Depth 6 -Compress
