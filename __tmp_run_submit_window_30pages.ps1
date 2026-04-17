$env:PYTHONPATH = "polymarket_rule_engine;."
$envFile = "version3.server.env"

Get-Content $envFile | Where-Object { $_ -match "^[^#]" } | ForEach-Object {
    $parts = $_ -split "=", 2
    if ($parts.Length -eq 2) {
        $name = $parts[0].Trim()
        $value = $parts[1].Trim().Trim("'").Trim('"')
        Set-Content "env:$name" $value
    }
}

execution_engine\app\scripts\online\run_submit_window.ps1 -MaxPages 30
