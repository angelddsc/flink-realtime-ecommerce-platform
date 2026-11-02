# 一键启动 MySQL + Kafka + Flink
$ErrorActionPreference = "Stop"
$here = $PSScriptRoot

& "$here\start-mysql.ps1"
Start-Sleep -Seconds 8

& "$here\start-kafka.ps1"
Write-Host "等待 Kafka 就绪..."
Start-Sleep -Seconds 15

& "$here\start-flink.ps1"
Write-Host ""
Write-Host "全部启动完成:"
Write-Host "  Flink Web UI : http://localhost:8081"
Write-Host "  下一步: 启动数据模拟器 start-producer.ps1, 提交作业 run-job.ps1, 大屏 start-dashboard.ps1"
