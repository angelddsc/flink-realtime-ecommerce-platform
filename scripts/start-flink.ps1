# 启动本机原生 Flink (JobManager + TaskManager)
# Windows 下 Flink 1.17 无 .bat 脚本, 这里用 java 直接拉起, 并用 BashJavaUtils 计算内存参数
# stdout/stderr 重定向到 log\flink-jm.out / flink-tm.out, 作业里 print() 的结果就在 flink-tm.out
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
$FLINK_HOME = if ($env:FLINK_HOME -and (Test-Path "$env:FLINK_HOME\lib")) { $env:FLINK_HOME } else { "D:\Graduation_Project\env\flink" }
$java = "$JAVA_HOME\bin\java.exe"

if (Get-NetTCPConnection -LocalPort 8081 -State Listen -ErrorAction SilentlyContinue) {
    Write-Host "[Flink] 已在运行 (8081)"
    exit 0
}

$libCp = (Get-ChildItem "$FLINK_HOME\lib\*.jar" | ForEach-Object { $_.FullName }) -join ';'
$dist  = (Get-ChildItem "$FLINK_HOME\lib\flink-dist*.jar").FullName
$bashUtilsCp = "$FLINK_HOME\bin\bash-java-utils.jar;$dist"

function Get-FlinkParams([string]$cmd) {
    $out = & $java -classpath $bashUtilsCp org.apache.flink.runtime.util.bash.BashJavaUtils $cmd --configDir "$FLINK_HOME\conf" 2>&1
    $prefix = "BASH_JAVA_UTILS_EXEC_RESULT:"
    $lines = $out | Where-Object { $_ -like "$prefix*" } | ForEach-Object { $_.Substring($prefix.Length).Trim() }
    if ($lines.Count -lt 2) { throw "无法获取 $cmd 参数: $out" }
    return $lines
}

# 读取 config.yaml 中 env.java.opts.all (Java 17 所需的 --add-opens/--add-exports)
function Get-JavaOptsAll {
    $m = Select-String -Path "$FLINK_HOME\conf\config.yaml" -Pattern '^\s*all:\s*(.+)$' | Select-Object -First 1
    if ($m) { return ($m.Matches.Groups[1].Value -split '\s+' | Where-Object { $_ }) }
    return @()
}

# 组装 java 参数数组:
#   JVM 参数(内存) + 日志参数 + -cp + 主类 + 动态程序参数(-D key=value) + --configDir
function Build-Args([string[]]$params, [string]$mainClass, [string]$logFile) {
    $a = @()
    $a += (Get-JavaOptsAll)
    $a += ($params[0].Split(' ') | Where-Object { $_ })
    $a += "-Dfile.encoding=UTF-8"
    $a += "-Dsun.stdout.encoding=UTF-8"
    $a += "-Dsun.stderr.encoding=UTF-8"
    $a += "-Dlog4j.configurationFile=file:$FLINK_HOME\conf\log4j.properties"
    $a += "-Dlogback.configurationFile=file:$FLINK_HOME\conf\logback.xml"
    $a += "-Dlog.file=$logFile"
    $a += "-cp"
    $a += $libCp
    $a += $mainClass
    # 动态参数必须放在主类之后, 形如 "-D" "key=value"
    $a += ($params[1].Split(' ') | Where-Object { $_ })
    $a += "--configDir"
    $a += "$FLINK_HOME\conf"
    return $a
}

function Start-Daemon([string]$batName, [string[]]$javaArgs, [string]$outFile) {
    $bat = Join-Path "$FLINK_HOME\bin" $batName
    $line = "`"$java`" " + ($javaArgs -join ' ') + " > `"$outFile`" 2>&1"
    # 用系统 ANSI(GBK) 编码写 .bat, 否则中文路径会变成 '?' 导致 cmd 无法解析
    $text = "@echo off`r`n" + $line + "`r`n"
    [System.IO.File]::WriteAllText($bat, $text, [System.Text.Encoding]::GetEncoding(936))
    Start-Process -FilePath $bat -WindowStyle Hidden
}

$jmArgs = Build-Args (Get-FlinkParams "GET_JM_RESOURCE_PARAMS") "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint" "$FLINK_HOME\log\flink-jm.log"
$tmArgs = Build-Args (Get-FlinkParams "GET_TM_RESOURCE_PARAMS") "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" "$FLINK_HOME\log\flink-tm.log"

Start-Daemon "win-start-jm.bat" $jmArgs "$FLINK_HOME\log\flink-jm.out"
Start-Sleep -Seconds 4
Start-Daemon "win-start-tm.bat" $tmArgs "$FLINK_HOME\log\flink-tm.out"

Write-Host "[Flink] JobManager + TaskManager 启动中..."
Write-Host "[Flink] Web UI: http://localhost:8081"
Write-Host "[Flink] 作业 print() 输出: $FLINK_HOME\log\flink-tm.out"
