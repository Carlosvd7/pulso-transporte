# Arranca, para o consulta el recolector en segundo plano (Windows, sin Docker).
#   .\scripts\collector.ps1 start | stop | status
param([ValidateSet("start", "stop", "status")][string]$Action = "status")

$root = Split-Path -Parent $PSScriptRoot
$pidFile = Join-Path $root "data\collector.pid"
$exe = Join-Path $root ".venv\Scripts\pulso.exe"

function Get-CollectorProcess {
    if (-not (Test-Path $pidFile)) { return $null }
    $id = [int](Get-Content $pidFile)
    Get-Process -Id $id -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -eq "pulso" }
}

switch ($Action) {
    "start" {
        if (Get-CollectorProcess) { Write-Output "Ya está en marcha."; break }
        New-Item -ItemType Directory -Force (Join-Path $root "data") | Out-Null
        $p = Start-Process -FilePath $exe -ArgumentList "collect" -WorkingDirectory $root `
            -WindowStyle Hidden -PassThru `
            -RedirectStandardError (Join-Path $root "data\collector.log") `
            -RedirectStandardOutput (Join-Path $root "data\collector.out")
        $p.Id | Out-File -Encoding ascii $pidFile
        Write-Output "Recolector iniciado (PID $($p.Id)). Log: data\collector.log"
    }
    "stop" {
        $p = Get-CollectorProcess
        if ($p) { Stop-Process -Id $p.Id; Write-Output "Recolector parado." }
        else { Write-Output "No estaba en marcha." }
    }
    "status" {
        $p = Get-CollectorProcess
        if ($p) { Write-Output "En marcha desde $($p.StartTime) (PID $($p.Id))." }
        else { Write-Output "Parado." }
        & $exe status
    }
}
