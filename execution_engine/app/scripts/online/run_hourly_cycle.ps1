param(
    [string]$RunId = "HOURLY_CYCLE",
    [string]$UniverseCsv = "",
    [int]$StreamDurationSec = 20,
    [int]$MaxBatches = 0,
    [int]$MarketLimit = 0,
    [int]$MonitorSleepSec = 0,
    [switch]$SkipRefreshUniverse,
    [switch]$SkipStream,
    [switch]$SkipSubmit,
    [switch]$SkipMonitor
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
    "run-hourly-cycle",
    "--run-id",
    $RunId,
    "--stream-duration-sec",
    $StreamDurationSec,
    "--monitor-sleep-sec",
    $MonitorSleepSec
)

if ($UniverseCsv) {
    $args += "--universe-csv"
    $args += $UniverseCsv
}

if ($MaxBatches -gt 0) {
    $args += "--max-batches"
    $args += $MaxBatches
}

if ($MarketLimit -gt 0) {
    $args += "--market-limit"
    $args += $MarketLimit
}

if ($SkipRefreshUniverse) {
    $args += "--skip-refresh-universe"
}
if ($SkipStream) {
    $args += "--skip-stream"
}
if ($SkipSubmit) {
    $args += "--skip-submit"
}
if ($SkipMonitor) {
    $args += "--skip-monitor"
}

Push-Location $repoRoot
try {
    & $venvPython @args
} finally {
    Pop-Location
}

