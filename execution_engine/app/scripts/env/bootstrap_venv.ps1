param(
    [switch]$ForceReinstall
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDir "..\\..\\..\\.."))
$venvDir = Join-Path $repoRoot ".venv-execution"
$venvPython = Join-Path $venvDir "Scripts\\python.exe"
$requirementsPath = Join-Path $repoRoot "execution_engine\\requirements-live.txt"
$clobClientPath = Join-Path $repoRoot "py-clob-client"
$clobClientGitUrl = if ($env:FORTUNE_BOT_PY_CLOB_CLIENT_GIT_URL) { $env:FORTUNE_BOT_PY_CLOB_CLIENT_GIT_URL } else { "https://github.com/Polymarket/py-clob-client.git" }
$clobClientRef = $env:FORTUNE_BOT_PY_CLOB_CLIENT_REF

function Install-ClobClient {
    param(
        [switch]$ForceReinstall
    )

    $hasLocalProject = (Test-Path (Join-Path $clobClientPath "setup.py")) -or (Test-Path (Join-Path $clobClientPath "pyproject.toml"))
    if ($hasLocalProject) {
        if ($ForceReinstall) {
            & $venvPython -m pip install --force-reinstall -e $clobClientPath
        } else {
            & $venvPython -m pip install -e $clobClientPath
        }
        return
    }

    $gitSpec = if ([string]::IsNullOrWhiteSpace($clobClientRef)) { "git+$clobClientGitUrl" } else { "git+$clobClientGitUrl@$clobClientRef" }
    if ($ForceReinstall) {
        & $venvPython -m pip install --force-reinstall $gitSpec
    } else {
        & $venvPython -m pip install $gitSpec
    }
}

if (-not (Test-Path $venvPython)) {
    Write-Host "Creating virtual environment at $venvDir"
    python -m venv $venvDir
}

Write-Host "Upgrading pip in $venvDir"
& $venvPython -m pip install --upgrade pip

if ($ForceReinstall) {
    Write-Host "Force reinstalling runtime dependencies"
    & $venvPython -m pip install --force-reinstall -r $requirementsPath
    Install-ClobClient -ForceReinstall
} else {
    Write-Host "Installing runtime dependencies"
    & $venvPython -m pip install -r $requirementsPath
    Install-ClobClient
}

Write-Host "Execution environment ready: $venvPython"
