# 启动实时监控大屏 (Flask)
$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$dir = Join-Path $ProjectRoot "dashboard"

Start-Process -FilePath "cmd.exe" -ArgumentList "/c","python app.py > dashboard.log 2>&1" -WorkingDirectory $dir -WindowStyle Hidden
Write-Host "[Dashboard] 启动中... http://localhost:5000"
