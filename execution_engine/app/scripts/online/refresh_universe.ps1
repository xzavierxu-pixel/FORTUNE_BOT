param(
    [string]$RunId = "UNIVERSE_REFRESH",
    [int]$MaxMarkets = 0,
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
    "refresh-universe",
    "--run-id",
    $RunId,
    "--print-head",
    $PrintHead
)

if ($MaxMarkets -gt 0) {
    $args += "--max-markets"
    $args += $MaxMarkets
}

Push-Location $repoRoot
try {
    & $venvPython @args
} finally {
    Pop-Location
}

