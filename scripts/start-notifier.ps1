# 启动告警通知推送器（钉钉/企业微信/通用 Webhook）
# 用法示例：
#   $env:WEBHOOK_URL="https://oapi.dingtalk.com/robot/send?access_token=xxx"
#   $env:WEBHOOK_TYPE="dingtalk"
#   powershell -ExecutionPolicy Bypass -File scripts\start-notifier.ps1
$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$dir = Join-Path $ProjectRoot "dashboard"

Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'notifier\.py' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

Start-Process -FilePath "cmd.exe" -ArgumentList "/c","python -u notifier.py > notifier.log 2>&1" -WorkingDirectory $dir -WindowStyle Hidden
if ($env:WEBHOOK_URL) {
    Write-Host "[notifier] 已启动，推送类型 $env:WEBHOOK_TYPE"
} else {
    Write-Host "[notifier] 已启动（未配置 WEBHOOK_URL，仅记录不推送）"
}
Write-Host "[notifier] 日志: $dir\notifier.log"
