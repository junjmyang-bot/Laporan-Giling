param(
    [switch]$Install,
    [switch]$CheckTelegram,
    [int]$Port = 8501
)

$ErrorActionPreference = "Stop"

function Load-EnvFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return }
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        if ($line.StartsWith("#")) { return }
        $eq = $line.IndexOf("=")
        if ($eq -lt 1) { return }
        $name = $line.Substring(0, $eq).Trim()
        $value = $line.Substring($eq + 1).Trim()
        [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

Load-EnvFile ".env"

if ($Install) {
    py -m pip install -r requirements.txt
}

if ($CheckTelegram) {
    py scripts/check_telegram.py
}

$conn = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -First 1
if ($conn) {
    Stop-Process -Id $conn.OwningProcess -Force
}

py -m streamlit run app.py --server.address 127.0.0.1 --server.port $Port
