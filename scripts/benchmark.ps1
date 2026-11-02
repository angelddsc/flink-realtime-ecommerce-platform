# Flink 实时链路压测脚本
# 用法: powershell -ExecutionPolicy Bypass -File scripts\benchmark.ps1 -Total 100000 -Threads 4
param(
    [int]$Total = 100000,
    [int]$Threads = 4
)
$ErrorActionPreference = "Continue"

$root   = Split-Path -Parent $PSScriptRoot
$sim    = Join-Path $root "data-simulator"
$java   = "D:\Graduation_Project\env\jdk-17\bin\java.exe"
$khome  = "D:\Graduation_Project\env\kafka"
$mysql  = "D:\Graduation_Project\env\mysql\bin\mysql.exe"
$cp     = (Get-ChildItem "$khome\libs\*.jar" | ForEach-Object { $_.FullName }) -join ';'

function Get-TotalOffset {
    $out = & $java "-Dlog4j2.configurationFile=file:$khome\config\log4j2.yaml" "-Dkafka.logs.dir=$khome\logs" -cp $cp org.apache.kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic order-events 2>$null
    $sum = 0
    foreach ($line in $out) {
        if ($line -match ':(\d+)\s*$') { $sum += [int]$Matches[1] }
    }
    return $sum
}

function Get-RunningJobId {
    $jo = (Invoke-WebRequest "http://127.0.0.1:8081/jobs/overview" -UseBasicParsing).Content | ConvertFrom-Json
    return ($jo.jobs | Where-Object { $_.state -eq 'RUNNING' } | Select-Object -First 1).jid
}

# 先停掉常规数据模拟器, 避免干扰
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match 'producer\.py' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
Start-Sleep -Seconds 3

$jobId = Get-RunningJobId
if (-not $jobId) { throw "没有正在运行的 Flink 作业, 请先执行 run-job.ps1" }

$offsetStart = Get-TotalOffset
$t0 = Get-Date
Write-Host ("[压测] 起始 offset = {0}" -f $offsetStart)

$env:TOTAL = $Total
$env:THREADS = $Threads
$env:PYTHONWARNINGS = "ignore"
Push-Location $sim
& python loadgen.py
Pop-Location
$tLoad = Get-Date

# 轮询等待 Flink 追平
$prev = -1
$stable = 0
while ($true) {
    Start-Sleep -Seconds 2
    $now = Get-TotalOffset
    if ($now -eq $prev) { $stable++ } else { $stable = 0 }
    $prev = $now
    if ($stable -ge 2) { break }
}
$tEnd = Get-Date
$offsetEnd = $prev

$produceSec = ($tLoad - $t0).TotalSeconds
$totalSec   = ($tEnd - $t0).TotalSeconds
$consumed   = $offsetEnd - $offsetStart
$flinkRate  = if ($totalSec -gt 0) { $consumed / $totalSec } else { 0 }

Write-Host ""
Write-Host "==================== 压测报告 ===================="
Write-Host ("发送总量         : {0} 条" -f $consumed)
Write-Host ("压测耗时         : {0:N2} s" -f $produceSec)
Write-Host ("Flink 端到端耗时 : {0:N2} s" -f $totalSec)
Write-Host ("Flink 处理速率   : {0:N0} 条/秒" -f $flinkRate)

Write-Host ("Kafka 累计消息   : {0} 条" -f $offsetEnd)

# 延迟统计 (最近 300 条)
$lat = & $mysql -u root -proot123456 -N -e "SELECT ROUND(AVG(latency_ms),1), MIN(latency_ms), MAX(latency_ms) FROM (SELECT latency_ms FROM bigdata.category_metric ORDER BY id DESC LIMIT 300) t;" 2>$null
$parts = $lat -split "`t"
Write-Host ("处理延迟(ms)     : 平均 {0} / 最小 {1} / 最大 {2}" -f $parts[0], $parts[1], $parts[2])
Write-Host "=================================================="

Write-Host ""
Write-Host "提示: 重新启动常规数据模拟器 -> scripts\start-producer.ps1"
