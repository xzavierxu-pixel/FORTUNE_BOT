param(
    [string]$RunId = "MARKET_STREAM",
    [int]$DurationSec = 60,
    [int]$MarketLimit = 0,
    [int]$MarketOffset = 0,
    [int]$PrintHead = 5,
    [string[]]$AssetId = @()
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
    "stream-market-data",
    "--run-id",
    $RunId,
    "--duration-sec",
    $DurationSec,
    "--market-offset",
    $MarketOffset,
    "--print-head",
    $PrintHead
)

if ($MarketLimit -gt 0) {
    $args += "--market-limit"
    $args += $MarketLimit
}

foreach ($token in $AssetId) {
    if ($token) {
        $args += "--asset-id"
        $args += $token
    }
}

Push-Location $repoRoot
try {
    & $venvPython @args
} finally {
    Pop-Location
}

