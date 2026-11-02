# 停止 Kafka / Flink / 数据模拟器
$ErrorActionPreference = "SilentlyContinue"
Get-Process java   | Stop-Process -Force
Get-Process python | Stop-Process -Force
Write-Host "已停止所有 Java (Kafka + Flink) 与 Python (模拟器) 进程"
