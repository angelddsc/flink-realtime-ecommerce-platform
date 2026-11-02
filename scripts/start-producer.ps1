# 启动数据模拟器：按时间戳回放淘宝 UserBehavior 数据集
$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$dir = Join-Path $ProjectRoot "data-simulator"

# 选择数据集：优先真实 UserBehavior.csv，其次联调样本
$dataFile = $env:DATA_FILE
if (-not $dataFile) {
    if (Test-Path (Join-Path $dir "UserBehavior.csv")) {
        $dataFile = Join-Path $dir "UserBehavior.csv"
    } elseif (Test-Path (Join-Path $dir "UserBehavior_sample.csv")) {
        $dataFile = Join-Path $dir "UserBehavior_sample.csv"
    }
}
if (-not $dataFile -or -not (Test-Path $dataFile)) {
    Write-Host "[producer] 未找到数据集，请把 UserBehavior.csv 放到 $dir 或设置环境变量 DATA_FILE"
    exit 1
}

# 停掉已有的 producer/replay 进程，避免重复发送
Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'producer\.py|replay\.py' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

$env:DATA_FILE       = $dataFile
$env:KAFKA_BOOTSTRAP = if ($env:KAFKA_BOOTSTRAP) { $env:KAFKA_BOOTSTRAP } else { "localhost:9092" }
$env:KAFKA_TOPIC     = if ($env:KAFKA_TOPIC)     { $env:KAFKA_TOPIC }     else { "order-events" }
$env:SPEED           = if ($env:SPEED)           { $env:SPEED }           else { "100" }
$env:ONLY_BUY        = if ($env:ONLY_BUY)        { $env:ONLY_BUY }        else { "0" }
$env:LOOP            = if ($env:LOOP)            { $env:LOOP }            else { "1" }
$env:EPS_CAP         = if ($env:EPS_CAP)         { $env:EPS_CAP }         else { "20000" }

Start-Process -FilePath "cmd.exe" -ArgumentList "/c","python -u replay.py > producer.log 2>&1" -WorkingDirectory $dir -WindowStyle Hidden
Write-Host "[producer] 回放已启动"
Write-Host "[producer] 数据集: $dataFile"
Write-Host "[producer] 倍速 SPEED=$env:SPEED, 循环 LOOP=$env:LOOP, 仅购买 ONLY_BUY=$env:ONLY_BUY"
Write-Host "[producer] 日志: $dir\producer.log"
