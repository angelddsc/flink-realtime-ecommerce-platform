# 启动本机原生 MySQL
$ErrorActionPreference = "Stop"

$MYSQL_HOME = if ($env:MYSQL_HOME -and (Test-Path "$env:MYSQL_HOME\bin\mysqld.exe")) { $env:MYSQL_HOME } else { "D:\Graduation_Project\env\mysql" }

if (Get-NetTCPConnection -LocalPort 3306 -State Listen -ErrorAction SilentlyContinue) {
    Write-Host "[MySQL] 已在运行 (3306)"
    exit 0
}

Start-Process -FilePath "$MYSQL_HOME\bin\mysqld.exe" `
    -ArgumentList "--defaults-file=`"$MYSQL_HOME\my.ini`"" -WindowStyle Hidden
Write-Host "[MySQL] 启动中... 127.0.0.1:3306（账号/密码见 README 环境变量）"
