Param(
    [string]$DataDir
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..\..\..")

if (-not $DataDir) {
    $DataDir = Join-Path $repoRoot "execution_engine\data"
}

$resolvedDataDir = [System.IO.Path]::GetFullPath($DataDir)
$expectedDataDir = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "execution_engine\data"))

if ($resolvedDataDir -ne $expectedDataDir) {
    throw "Refusing to clear unexpected path: $resolvedDataDir"
}

if (-not (Test-Path -LiteralPath $resolvedDataDir)) {
    New-Item -ItemType Directory -Path $resolvedDataDir | Out-Null
}

Write-Host "[INFO] Clearing execution data directory: $resolvedDataDir"

Get-ChildItem -LiteralPath $resolvedDataDir -Force | Remove-Item -Recurse -Force

$subdirs = @("runs", "shared", "summary", "tests")
foreach ($subdir in $subdirs) {
    $path = Join-Path $resolvedDataDir $subdir
    New-Item -ItemType Directory -Path $path -Force | Out-Null
}

Write-Host "[INFO] Execution data directory reset complete."
