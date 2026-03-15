param(
    [string]$RunId = "SNAPSHOT_SCORE",
    [int]$MarketLimit = 0,
    [int]$MarketOffset = 0,
    [int]$PrintHead = 5,
    [string]$UniverseCsv = "",
    [string]$TokenStateCsv = ""
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
    "score-hourly",
    "--run-id",
    $RunId,
    "--market-offset",
    $MarketOffset,
    "--print-head",
    $PrintHead
)

if ($MarketLimit -gt 0) {
    $args += "--market-limit"
    $args += $MarketLimit
}

if ($UniverseCsv) {
    $args += "--universe-csv"
    $args += $UniverseCsv
}

if ($TokenStateCsv) {
    $args += "--token-state-csv"
    $args += $TokenStateCsv
}

Push-Location $repoRoot
try {
    & $venvPython @args
} finally {
    Pop-Location
}

