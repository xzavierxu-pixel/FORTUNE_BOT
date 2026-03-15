param(
    [string]$RunId = "ORDER_MONITOR",
    [int]$SleepSec = 0
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDir "..\\..\\..\\.."))
$venvPython = Join-Path $repoRoot ".venv-execution\\Scripts\\python.exe"
$bootstrapScript = Join-Path $repoRoot "execution_engine\\app\\scripts\\env\\bootstrap_venv.ps1"

if (-not (Test-Path $venvPython)) {
    & $bootstrapScript
}

$args = @(
    "-m",
    "execution_engine.app.cli.online.main",
    "monitor-orders",
    "--run-id",
    $RunId
)

if ($SleepSec -gt 0) {
    $args += "--sleep-sec"
    $args += $SleepSec
}

Push-Location $repoRoot
try {
    & $venvPython @args
} finally {
    Pop-Location
}

