param(
    [string]$RunId = "SUBMIT_HOURLY",
    [string]$SelectionCsv = "",
    [string]$TokenStateCsv = "",
    [int]$MaxOrders = 0,
    [int]$PrintHead = 5
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
    "submit-hourly",
    "--run-id",
    $RunId,
    "--print-head",
    $PrintHead
)

if ($SelectionCsv) {
    $args += "--selection-csv"
    $args += $SelectionCsv
}

if ($TokenStateCsv) {
    $args += "--token-state-csv"
    $args += $TokenStateCsv
}

if ($MaxOrders -gt 0) {
    $args += "--max-orders"
    $args += $MaxOrders
}

Push-Location $repoRoot
try {
    & $venvPython @args
} finally {
    Pop-Location
}

