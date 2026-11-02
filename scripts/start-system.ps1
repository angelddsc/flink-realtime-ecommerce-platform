# 一键启动整个系统（幂等：已在运行的组件会自动跳过）
$ErrorActionPreference = "Stop"
$here = $PSScriptRoot

Write-Host "==== 启动 MySQL / Kafka / Flink ===="
& "$here\start-all.ps1"

Write-Host ""
Write-Host "==== 等待 Flink 就绪 ===="
Start-Sleep -Seconds 12

# 提交 Flink 作业（若已有运行中的作业则跳过）
$hasJob = $false
try {
    $ov = (Invoke-WebRequest 'http://127.0.0.1:8081/jobs/overview' -UseBasicParsing -TimeoutSec 5).Content | ConvertFrom-Json
    $hasJob = (($ov.jobs | Where-Object { $_.state -eq 'RUNNING' } | Measure-Object).Count -gt 0)
} catch {}
if ($hasJob) {
    Write-Host "[job] 已有作业在运行，跳过提交"
} else {
    & "$here\run-job.ps1"
}

# 数据模拟器
$producerRunning = (Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'producer\.py' } | Measure-Object).Count -gt 0
if ($producerRunning) {
    Write-Host "[producer] 已在运行，跳过"
} else {
    & "$here\start-producer.ps1"
}

# 监控大屏
if (Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue) {
    Write-Host "[dashboard] 已在运行，跳过"
} else {
    & "$here\start-dashboard.ps1"
}

Start-Sleep -Seconds 5
Write-Host ""
Write-Host "========================================"
Write-Host " Flink Web UI : http://localhost:8081"
Write-Host " 监控大屏     : http://localhost:5000"
Write-Host " 停止系统     : scripts\stop-all.ps1"
Write-Host "========================================"
