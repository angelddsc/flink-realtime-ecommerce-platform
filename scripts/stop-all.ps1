# 停止本项目相关服务（MySQL / Kafka / Flink / 回放器 / 大屏 / 通知器）
# 只匹配本项目进程，避免误杀其它 Java / Python 程序
$ErrorActionPreference = "SilentlyContinue"

$MYSQL_HOME = if ($env:MYSQL_HOME) { $env:MYSQL_HOME } else { "D:\Graduation_Project\env\mysql" }

# 1) 若设置了 MYSQL_PASSWORD，则优雅关闭 MySQL；否则直接结束 mysqld
if ($env:MYSQL_PASSWORD -and (Test-Path "$MYSQL_HOME\bin\mysqladmin.exe")) {
    & "$MYSQL_HOME\bin\mysqladmin.exe" -u root "-p$env:MYSQL_PASSWORD" shutdown 2>$null | Out-Null
    Start-Sleep -Seconds 2
}

# 2) 停止本项目相关的 Java（Kafka / Flink）
Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
    Where-Object { $_.CommandLine -match 'env\\kafka|kafka\.Kafka|env\\flink|StandaloneSessionClusterEntrypoint|TaskManagerRunner' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

# 3) 停止本项目相关的 Python（回放器 / 大屏 / 通知器 / 压测）
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match 'replay\.py|producer\.py|loadgen\.py|app\.py|notifier\.py' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

# 4) 兜底：结束残留 mysqld
Get-Process mysqld -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "已停止本项目相关服务（MySQL / Kafka / Flink / 回放器 / 大屏 / 通知器）"
