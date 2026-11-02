# 编译 Flink 作业并通过 REST 提交到本机 Flink 集群
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
$MAVEN_HOME = if ($env:MAVEN_HOME -and (Test-Path "$env:MAVEN_HOME\bin\mvn.cmd")) { $env:MAVEN_HOME } else { "D:\Graduation_Project\env\maven" }
$env:JAVA_HOME = $JAVA_HOME
$env:Path = "$JAVA_HOME\bin;$MAVEN_HOME\bin;$env:Path"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$JobDir = Join-Path $ProjectRoot "flink-job"
$Jar = Join-Path $JobDir "target\realtime-monitor-1.0.0.jar"

Write-Host "[1/3] 编译打包..."
Push-Location $JobDir
& mvn -q clean package
Pop-Location
if (-not (Test-Path $Jar)) { throw "构建失败: 未生成 $Jar" }

# 确保 Kafka 主题 order-events 存在（提交前建好，避免源报 UnknownTopicOrPartition）
$khome = "D:\Graduation_Project\env\kafka"
$kcp = (Get-ChildItem "$khome\libs\*.jar" | ForEach-Object { $_.FullName }) -join ';'
try { & "$env:JAVA_HOME\bin\java.exe" -cp $kcp org.apache.kafka.tools.TopicCommand --create --topic order-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 *> $null } catch {}

Write-Host "[2/3] 上传 jar 到 Flink..."
# 复制到 ASCII 临时路径, 避免项目路径含空格/中文导致 curl 解析异常
$tmpJar = Join-Path $env:TEMP "realtime-monitor-1.0.0.jar"
Copy-Item -LiteralPath $Jar -Destination $tmpJar -Force
$uploadJson = curl.exe -s -X POST -F "jarfile=@$tmpJar" http://localhost:8081/jars/upload
$upload = $uploadJson | ConvertFrom-Json
if (-not $upload.filename) { throw "上传失败: $uploadJson" }
$jarId = Split-Path -Leaf $upload.filename
Write-Host "      jarId = $jarId"

Write-Host "[3/3] 提交作业..."
$body = @{ entryClass = "com.grad.realtime.OrderRealtimeJob" } | ConvertTo-Json -Compress
$run = Invoke-RestMethod -Uri "http://localhost:8081/jars/$jarId/run" -Method Post -ContentType "application/json" -Body $body
Write-Host "作业已提交, jobId = $($run.jobid)"
