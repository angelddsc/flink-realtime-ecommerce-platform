# 启动本机原生 Kafka (KRaft 模式, 无需 ZooKeeper)
$ErrorActionPreference = "Stop"

function Resolve-JavaHome {
    param($cand, $fallback)
    if ($cand -and (Test-Path "$cand\release")) {
        $rel = Get-Content "$cand\release" -Raw -ErrorAction SilentlyContinue
        if ($rel -match 'JAVA_VERSION="(?:1\.)?([0-9]+)') { if ([int]$Matches[1] -ge 11) { return $cand } }
    }
    return $fallback
}
$JAVA_HOME  = Resolve-JavaHome $env:JAVA_HOME "D:\Graduation_Project\env\jdk-17"
$KAFKA_HOME = if ($env:KAFKA_HOME -and (Test-Path "$env:KAFKA_HOME\libs")) { $env:KAFKA_HOME } else { "D:\Graduation_Project\env\kafka" }
$java = "$JAVA_HOME\bin\java.exe"

if (Get-NetTCPConnection -LocalPort 9092 -State Listen -ErrorAction SilentlyContinue) {
    Write-Host "[Kafka] 已在运行 (9092)"
    exit 0
}

New-Item -ItemType Directory -Force -Path "$KAFKA_HOME\logs" | Out-Null

$args = @(
    "-Xmx1G", "-Xms1G",
    "-server", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=20",
    "-XX:InitiatingHeapOccupancyPercent=35", "-XX:+ExplicitGCInvokesConcurrent",
    "-Djava.awt.headless=true",
    "-Dkafka.logs.dir=$KAFKA_HOME\logs",
    "-cp", "$KAFKA_HOME\libs\*",
    "kafka.Kafka", "$KAFKA_HOME\config\server.properties"
)

$out = "$KAFKA_HOME\logs\kafka-console.out"
$bat = "$KAFKA_HOME\start-kafka.bat"
$line = "`"$java`" " + ($args -join ' ') + " > `"$out`" 2>&1"
[System.IO.File]::WriteAllText($bat, "@echo off`r`n" + $line + "`r`n", [System.Text.Encoding]::GetEncoding(936))
Start-Process -FilePath $bat -WindowStyle Hidden
Write-Host "[Kafka] 启动中... 日志: $out"
