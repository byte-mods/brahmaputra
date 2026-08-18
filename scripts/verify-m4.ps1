# M4 live verification (consumer groups): group assignment spread, mid-stream
# consumer kill with rebalance, coordinator-broker failover with committed
# offsets, and bounded-run resume from committed positions.
#
# The cluster has five combined nodes. The internal __consumer_offsets topic
# is pinned to six RF=3 partitions before group traffic so any group's
# coordinator survives one hard-killed broker; the data topic has six RF=3
# partitions for the three-consumer split.
#
# This script targets Windows PowerShell 5 as well as newer PowerShell:
#
#   powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\verify-m4.ps1

[CmdletBinding()]
param(
    [int]$TimeoutSeconds = 120,
    [int]$HeartbeatIntervalMs = 500,
    [int]$SessionTimeoutMs = 5000,
    [int]$SegmentBytes = 1048576,
    [int]$CliWallTimeoutSeconds = 180,
    [int]$ConsumerWallTimeoutSeconds = 900,
    [int]$OffsetsTopicPartitions = 6,
    [int]$TopicPartitions = 6,
    [int]$CommitIntervalMs = 300
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

$script:Checks = 0
$script:Succeeded = $false
$script:NodeCount = 5
$script:Nodes = @{}
$script:AllocatedPorts = New-Object 'System.Collections.Generic.HashSet[int]'
$script:ProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$script:WorkDir = Join-Path ([IO.Path]::GetTempPath()) ("brahmaputra-m4-" + [Guid]::NewGuid().ToString("N"))
$script:ClusterId = "m4-live-" + [Guid]::NewGuid().ToString("N").Substring(0, 12)
$script:Topic = "consumer-groups-live"
$script:OffsetsTopic = "__consumer_offsets"
$script:GroupSpread = "m4-spread"
$script:GroupFailover = "m4-failover"
$script:GroupResume = "m4-resume"
$script:ServerExe = $null
$script:CliExe = $null
$script:NodePorts = @{}
$script:ConsumerSequence = 0
$script:Consumers = @{}

function Write-Stage {
    param([string]$Message)
    Write-Host ""
    Write-Host ("==> " + $Message) -ForegroundColor Cyan
}

function Pass {
    param([string]$Message)
    $script:Checks++
    Write-Host ("PASS: " + $Message) -ForegroundColor Green
}

function Assert-True {
    param(
        [bool]$Condition,
        [string]$Message
    )
    if (-not $Condition) {
        throw "assertion failed: $Message"
    }
    Pass $Message
}

function Get-FreeTcpPort {
    while ($true) {
        $listener = New-Object System.Net.Sockets.TcpListener([Net.IPAddress]::Loopback, 0)
        try {
            $listener.Start()
            $port = ([Net.IPEndPoint]$listener.LocalEndpoint).Port
        }
        finally {
            $listener.Stop()
        }
        if ($script:AllocatedPorts.Add($port)) {
            return $port
        }
    }
}

function Quote-ProcessArgument {
    param([string]$Value)
    if ($Value -notmatch '[\s"]') {
        return $Value
    }
    return '"' + $Value.Replace('"', '\"') + '"'
}

function Get-MapValue {
    param(
        [object]$Map,
        [object]$Key
    )
    if ($null -eq $Map) {
        return $null
    }
    $keyText = [string]$Key
    $property = @($Map.PSObject.Properties | Where-Object { $_.Name -eq $keyText })
    if ($property.Count -eq 0) {
        return $null
    }
    return $property[0].Value
}

function Wait-ForValue {
    param(
        [string]$Description,
        [scriptblock]$Probe,
        [int]$Seconds = $script:TimeoutSeconds,
        [int]$DelayMilliseconds = 100
    )
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastError = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $value = & $Probe
            if ($null -ne $value -and $false -ne $value) {
                return $value
            }
        }
        catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds $DelayMilliseconds
    }
    if ($null -ne $lastError) {
        throw "timed out waiting for $Description; last error: $lastError"
    }
    throw "timed out waiting for $Description"
}

function Invoke-ControllerGet {
    param(
        [int]$NodeId,
        [string]$Path
    )
    $node = $script:Nodes[$NodeId]
    $uri = "http://127.0.0.1:$($node.ControlPort)$Path"
    return Invoke-RestMethod -Uri $uri -Method Get -TimeoutSec 4
}

function Invoke-ControllerPost {
    param(
        [int]$NodeId,
        [string]$Path
    )
    $node = $script:Nodes[$NodeId]
    $uri = "http://127.0.0.1:$($node.ControlPort)$Path"
    return Invoke-RestMethod -Uri $uri -Method Post -TimeoutSec 20
}

function Get-ResultProperty {
    param(
        [object]$Response,
        [string]$Name
    )
    if ($null -eq $Response) {
        return $null
    }
    $property = @($Response.PSObject.Properties | Where-Object { $_.Name -eq $Name })
    if ($property.Count -eq 0) {
        return $null
    }
    return $property[0]
}

function Assert-ControllerResultOk {
    param(
        [object]$Response,
        [string]$Description
    )
    $ok = Get-ResultProperty $Response "Ok"
    if ($null -eq $ok) {
        $details = $Response | ConvertTo-Json -Depth 20 -Compress
        throw "$Description was rejected: $details"
    }
    Pass $Description
    return $ok.Value
}

function Start-CombinedNode {
    param(
        [int]$NodeId,
        [switch]$Restart
    )
    $nodeDir = Join-Path $script:WorkDir ("node-" + $NodeId)
    $dataDir = Join-Path $nodeDir "data"
    $suffix = if ($Restart) { ".restart-$(([DateTime]::UtcNow).Ticks)" } else { "" }
    $stdout = Join-Path $nodeDir ("server$suffix.stdout.log")
    $stderr = Join-Path $nodeDir ("server$suffix.stderr.log")
    $null = New-Item -ItemType Directory -Force -Path $dataDir

    $arguments = @(
        "--host", "127.0.0.1",
        "--port", [string]$script:NodePorts[$NodeId].Data,
        "--data-dir", $dataDir,
        "--segment-bytes", [string]$SegmentBytes,
        "--node-id", [string]$NodeId,
        "--cluster-id", $script:ClusterId,
        "--control-port", [string]$script:NodePorts[$NodeId].Control,
        "--heartbeat-interval-ms", [string]$HeartbeatIntervalMs,
        "--session-timeout-ms", [string]$SessionTimeoutMs,
        "--offsets-topic-partitions", [string]$OffsetsTopicPartitions
    )
    foreach ($peerId in 1..$script:NodeCount) {
        $arguments += @(
            "--controller-peer",
            ("{0}=127.0.0.1:{1}" -f $peerId, $script:NodePorts[$peerId].Control)
        )
    }
    $argumentLine = (($arguments | ForEach-Object { Quote-ProcessArgument ([string]$_) }) -join " ")
    $process = Start-Process -FilePath $script:ServerExe `
        -ArgumentList $argumentLine `
        -WorkingDirectory $script:ProjectRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru

    $descriptor = [PSCustomObject]@{
        NodeId = $NodeId
        DataPort = [int]$script:NodePorts[$NodeId].Data
        ControlPort = [int]$script:NodePorts[$NodeId].Control
        DataDir = $dataDir
        Stdout = $stdout
        Stderr = $stderr
        Arguments = $argumentLine
        Process = $process
    }
    $script:Nodes[$NodeId] = $descriptor
    Write-Host ("started node {0}: pid={1} data={2} control={3}" -f $NodeId, $process.Id, $descriptor.DataPort, $descriptor.ControlPort)
    return $descriptor
}

function Stop-CombinedNode {
    param(
        [int]$NodeId,
        [switch]$Force
    )
    if (-not $script:Nodes.ContainsKey($NodeId)) {
        return
    }
    $process = $script:Nodes[$NodeId].Process
    try {
        $process.Refresh()
        if (-not $process.HasExited) {
            if ($Force) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
            else {
                Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
            }
            $null = $process.WaitForExit(5000)
            $process.Refresh()
            if (-not $process.HasExited) {
                # Hard fallback for a wedged child on Windows.
                $null = & taskkill.exe /PID $process.Id /T /F 2>$null
                $null = $process.WaitForExit(3000)
            }
        }
    }
    catch {
        Write-Warning ("could not stop node {0} pid {1}: {2}" -f $NodeId, $process.Id, $_.Exception.Message)
    }
}

function Wait-ForHttpReady {
    param([int[]]$NodeIds)
    foreach ($nodeId in $NodeIds) {
        $null = Wait-ForValue "node $nodeId controller HTTP readiness" {
            $node = $script:Nodes[$nodeId]
            $node.Process.Refresh()
            if ($node.Process.HasExited) {
                throw "node $nodeId exited with code $($node.Process.ExitCode)"
            }
            $raft = Invoke-ControllerGet $nodeId "/api/v1/controller/raft"
            if ($null -ne $raft) { return $true }
            return $null
        } 20 100
    }
}

function Wait-ForSharedLeader {
    param([int[]]$NodeIds)
    return Wait-ForValue "controllers $($NodeIds -join ',') to agree on a live Raft leader" {
        $leaders = @()
        foreach ($nodeId in $NodeIds) {
            $raft = Invoke-ControllerGet $nodeId "/api/v1/controller/raft"
            if ($null -eq $raft.current_leader) {
                return $null
            }
            $leaders += [int]$raft.current_leader
        }
        $unique = @($leaders | Select-Object -Unique)
        if ($unique.Count -eq 1 -and $NodeIds -contains $unique[0]) {
            return [int]$unique[0]
        }
        return $null
    }
}

function Wait-ForMetadataPredicate {
    param(
        [int[]]$NodeIds,
        [string]$Description,
        [scriptblock]$Predicate
    )
    return Wait-ForValue $Description {
        $images = @()
        foreach ($nodeId in $NodeIds) {
            $images += ,(Invoke-ControllerGet $nodeId "/api/v1/controller/metadata")
        }
        foreach ($image in $images) {
            if (-not (& $Predicate $image)) {
                return $null
            }
        }
        return $images[0]
    }
}

function Test-BrokersAlive {
    param(
        [object]$Image,
        [int[]]$BrokerIds,
        [bool]$ExpectedAlive
    )
    foreach ($brokerId in $BrokerIds) {
        $broker = Get-MapValue $Image.brokers $brokerId
        if ($null -eq $broker -or [bool]$broker.alive -ne $ExpectedAlive) {
            return $false
        }
    }
    return $true
}

function Test-TopicReady {
    param(
        [object]$Image,
        [string]$Name,
        [int]$Partitions,
        [int]$ReplicationFactor
    )
    $topic = Get-MapValue $Image.topics $Name
    if ($null -eq $topic) {
        return $false
    }
    if ((Get-MapEntries $topic.partitions).Count -ne $Partitions) {
        return $false
    }
    foreach ($entry in @(Get-MapEntries $topic.partitions)) {
        $partition = $entry.Value
        if ([int]$partition.leader -lt 0 -or @($partition.replicas).Count -ne $ReplicationFactor) {
            return $false
        }
    }
    return $true
}

function Get-MapEntries {
    param([object]$Map)
    if ($null -eq $Map) {
        return @()
    }
    return @($Map.PSObject.Properties)
}

function Get-PartitionLeader {
    param(
        [int]$NodeId,
        [string]$TopicName,
        [int]$Partition
    )
    $image = Invoke-ControllerGet $NodeId "/api/v1/controller/metadata"
    $topic = Get-MapValue $image.topics $TopicName
    if ($null -eq $topic) {
        return $null
    }
    $partition = Get-MapValue $topic.partitions $Partition
    if ($null -eq $partition) {
        return $null
    }
    return [int]$partition.leader
}

function Invoke-CliRaw {
    param([string[]]$Arguments)
    $lines = @(& $script:CliExe @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Text = $text
    }
}

function Invoke-Cli {
    param([string[]]$Arguments)
    $result = Invoke-CliRaw $Arguments
    if ($result.ExitCode -ne 0) {
        throw "CLI exited with code $($result.ExitCode) while running '$($Arguments -join ' ')':`n$($result.Text)"
    }
    return $result.Text
}

function Get-BrokerAddress {
    param([int]$NodeId)
    return "127.0.0.1:$($script:Nodes[$NodeId].DataPort)"
}

# --- consumer-group helpers -------------------------------------------------

function Start-GroupConsumer {
    param(
        [int]$SeedNodeId,
        [string]$Group
    )
    $script:ConsumerSequence++
    $id = $script:ConsumerSequence
    $consumerDir = Join-Path $script:WorkDir "consumers"
    $null = New-Item -ItemType Directory -Force -Path $consumerDir
    $stdout = Join-Path $consumerDir ("consumer-$id.stdout.log")
    $stderr = Join-Path $consumerDir ("consumer-$id.stderr.log")
    $arguments = @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "consume", "--topic", $script:Topic,
        "--group", $Group,
        "--follow",
        "--commit-interval-ms", [string]$CommitIntervalMs
    )
    $argumentLine = (($arguments | ForEach-Object { Quote-ProcessArgument ([string]$_) }) -join " ")
    $process = Start-Process -FilePath $script:CliExe `
        -ArgumentList $argumentLine `
        -WorkingDirectory $script:ProjectRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru
    $descriptor = [PSCustomObject]@{
        Id = $id
        Group = $Group
        Stdout = $stdout
        Stderr = $stderr
        Arguments = $argumentLine
        Process = $process
    }
    $script:Consumers[$id] = $descriptor
    Write-Host ("started consumer {0}: group={1} pid={2}" -f $id, $Group, $process.Id)
    return $descriptor
}

function Stop-GroupConsumer {
    param([int]$Id)
    if (-not $script:Consumers.ContainsKey($Id)) {
        return
    }
    $process = $script:Consumers[$Id].Process
    try {
        $process.Refresh()
        if (-not $process.HasExited) {
            # The CLI has no graceful signal path; stop hard, with a taskkill
            # fallback for a wedged child.
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            $null = $process.WaitForExit(5000)
            $process.Refresh()
            if (-not $process.HasExited) {
                $null = & taskkill.exe /PID $process.Id /T /F 2>$null
                $null = $process.WaitForExit(3000)
            }
        }
    }
    catch {
        Write-Warning ("could not stop consumer {0} pid {1}: {2}" -f $Id, $process.Id, $_.Exception.Message)
    }
}

function Get-LastAssignment {
    param([string]$Path)
    $last = $null
    $pattern = '^assignment: ' + [Regex]::Escape($script:Topic) + '=\[([0-9,]+)\]\s*$'
    foreach ($line in @(Get-Content -LiteralPath $Path -ErrorAction SilentlyContinue)) {
        $match = [Regex]::Match($line, $pattern)
        if ($match.Success) {
            $last = $match.Groups[1].Value
        }
    }
    if ($null -eq $last) {
        return $null
    }
    return @($last -split ',' | ForEach-Object { [int]$_ })
}

# The last assignment line of each consumer log must show exactly $Each
# partitions, all logs together covering 0..TopicPartitions-1 disjointly.
function Test-AssignmentSplit {
    param(
        [string[]]$Logs,
        [int]$Each
    )
    $seen = @{}
    $covered = 0
    foreach ($log in $Logs) {
        if (-not (Test-Path -LiteralPath $log)) {
            return $false
        }
        $parts = Get-LastAssignment $log
        if ($null -eq $parts -or $parts.Count -ne $Each) {
            return $false
        }
        foreach ($partition in $parts) {
            if ($partition -ge $script:TopicPartitions -or $seen.ContainsKey($partition)) {
                return $false
            }
            $seen[$partition] = $true
        }
        $covered += $parts.Count
    }
    return ($covered -eq $script:TopicPartitions -and $seen.Count -eq $script:TopicPartitions)
}

function Get-ConsumedValues {
    param([string[]]$Logs)
    $values = @()
    foreach ($log in $Logs) {
        if (-not (Test-Path -LiteralPath $log)) {
            continue
        }
        foreach ($line in @(Get-Content -LiteralPath $log -ErrorAction SilentlyContinue)) {
            $match = [Regex]::Match($line, '^partition=\d+ offset=\d+ key=.*? value=(\S+)\s*$')
            if ($match.Success) {
                $values += $match.Groups[1].Value
            }
        }
    }
    return $values
}

# Every value in the file must appear exactly once (Mode "once") or at least
# once (Mode "any") across the given consumer logs.
function Test-ValuesConsumed {
    param(
        [string]$Mode,
        [string]$ValuesFile,
        [string[]]$Logs
    )
    $want = @{}
    foreach ($value in @(Get-Content -LiteralPath $ValuesFile)) {
        $trimmed = $value.Trim()
        if ($trimmed.Length -gt 0) {
            $want[$trimmed] = $true
        }
    }
    $counts = @{}
    foreach ($value in @(Get-ConsumedValues $Logs)) {
        if ($want.ContainsKey($value)) {
            $counts[$value] = 1 + [int]$counts[$value]
        }
    }
    foreach ($key in @($want.Keys)) {
        $n = 0
        if ($counts.ContainsKey($key)) {
            $n = [int]$counts[$key]
        }
        if ($Mode -eq "once" -and $n -ne 1) {
            return $false
        }
        if ($Mode -eq "any" -and $n -lt 1) {
            return $false
        }
    }
    return $true
}

function Test-RecordsSeen {
    param(
        [int]$Minimum,
        [string[]]$Logs
    )
    $seen = 0
    foreach ($log in $Logs) {
        if (-not (Test-Path -LiteralPath $log)) {
            continue
        }
        foreach ($line in @(Get-Content -LiteralPath $log -ErrorAction SilentlyContinue)) {
            if ($line -match '^partition=\d+ offset=\d+ key=') {
                $seen++
            }
        }
    }
    return ($seen -ge $Minimum)
}

# Redelivery is only legal inside the kill window: every value consumed more
# than once must have been consumed by the killed member before it died.
function Assert-DupsFromVictim {
    param(
        [string]$VictimLog,
        [string[]]$OtherLogs,
        [string]$Description
    )
    $victimValues = @{}
    foreach ($value in @(Get-ConsumedValues @($VictimLog))) {
        $victimValues[$value] = $true
    }
    $counts = @{}
    foreach ($value in @(Get-ConsumedValues (@($VictimLog) + $OtherLogs))) {
        $counts[$value] = 1 + [int]$counts[$value]
    }
    foreach ($key in @($counts.Keys)) {
        if ([int]$counts[$key] -gt 1 -and -not $victimValues.ContainsKey($key)) {
            throw "assertion failed: duplicate $key was never consumed by the killed member"
        }
    }
    Pass $Description
}

# None of the values in the file may appear in the log (proves a resumed
# consumer did not rewind to earliest).
function Assert-ValuesAbsent {
    param(
        [string]$ValuesFile,
        [string]$Log,
        [string]$Description
    )
    $old = @{}
    foreach ($value in @(Get-Content -LiteralPath $ValuesFile)) {
        $trimmed = $value.Trim()
        if ($trimmed.Length -gt 0) {
            $old[$trimmed] = $true
        }
    }
    foreach ($value in @(Get-ConsumedValues @($Log))) {
        if ($old.ContainsKey($value)) {
            throw "assertion failed: pre-failover record $value was redelivered after the committed resume"
        }
    }
    Pass $Description
}

# A foreground consume run must print exactly the values in the file, each
# once, with every record offset >= MinOffset.
function Assert-ResumeRun {
    param(
        [string]$RunText,
        [string]$ValuesFile,
        [Int64]$MinOffset,
        [string]$Description
    )
    $want = @{}
    foreach ($value in @(Get-Content -LiteralPath $ValuesFile)) {
        $trimmed = $value.Trim()
        if ($trimmed.Length -gt 0) {
            $want[$trimmed] = $true
        }
    }
    $seen = @{}
    $records = 0
    foreach ($line in @($RunText -split "`r?`n")) {
        $match = [Regex]::Match($line, '^partition=(\d+) offset=(\d+) key=.*? value=(\S+)\s*$')
        if (-not $match.Success) {
            continue
        }
        $partition = [int]$match.Groups[1].Value
        $offset = [Int64]$match.Groups[2].Value
        $value = $match.Groups[3].Value
        if ($offset -lt $MinOffset) {
            throw "assertion failed: run consumed offset $offset below committed position $MinOffset on partition $partition"
        }
        if (-not $want.ContainsKey($value)) {
            throw "assertion failed: run consumed unexpected value $value"
        }
        if ($seen.ContainsKey($value)) {
            throw "assertion failed: run consumed $value twice"
        }
        $seen[$value] = $true
        $records++
    }
    if ($records -ne $want.Count) {
        throw "assertion failed: run consumed $records records, expected $($want.Count)"
    }
    Pass $Description
}

function New-ValuesFile {
    param(
        [string]$Path,
        [string]$Prefix,
        [int]$Count
    )
    $lines = @()
    for ($i = 0; $i -lt $Count; $i++) {
        $lines += "{0}-{1:D4}" -f $Prefix, $i
    }
    [IO.File]::WriteAllLines($Path, $lines)
}

function Send-ValuesFile {
    param(
        [int]$SeedNodeId,
        [string]$Path,
        [int]$Count,
        [string]$Topic = $script:Topic
    )
    $output = Invoke-Cli @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "produce", "--topic", $Topic,
        "--file", $Path,
        "--acks", "all", "--timeout-ms", "60000"
    )
    Assert-True ($output.Contains("produced $Count records")) "produced $Count unique records"
}

function Get-Crc32C {
    param(
        [byte[]]$Bytes,
        [int]$Offset,
        [int]$Count
    )
    if ($Offset -lt 0 -or $Count -lt 0 -or $Offset -gt ($Bytes.Length - $Count)) {
        throw "invalid CRC32C range offset=$Offset count=$Count length=$($Bytes.Length)"
    }
    [UInt32]$crc = [UInt32]::MaxValue
    for ($index = $Offset; $index -lt ($Offset + $Count); $index++) {
        $crc = [UInt32]($crc -bxor [UInt32]($Bytes[$index]))
        for ($bit = 0; $bit -lt 8; $bit++) {
            if (($crc -band [UInt32]1) -ne 0) {
                $crc = [UInt32](($crc -shr 1) -bxor [UInt32]2197175160)
            }
            else {
                $crc = [UInt32]($crc -shr 1)
            }
        }
    }
    return [UInt32]($crc -bxor [UInt32]::MaxValue)
}

# crc32c(group_id) % offsets-partitions, matching the broker/client routing.
function Get-OffsetsPartition {
    param([string]$Group)
    $bytes = [Text.Encoding]::UTF8.GetBytes($Group)
    return [int]((Get-Crc32C $bytes 0 $bytes.Length) % [UInt32]$OffsetsTopicPartitions)
}

# The server creates __consumer_offsets as soon as its first node registers,
# possibly with RF=1 when the rest of the cluster has not registered yet.
# Before any group traffic, pin it to the full RF=3 layout by deleting and
# recreating it until the metadata image converges.
function Invoke-PinOffsetsTopic {
    param(
        [int[]]$NodeIds,
        [int]$ControllerNodeId
    )
    $controller = "http://127.0.0.1:$($script:Nodes[$ControllerNodeId].ControlPort)"
    $null = Wait-ForValue "$($script:OffsetsTopic) to converge to $OffsetsTopicPartitions RF=3 partitions" {
        $ready = $true
        foreach ($nodeId in $NodeIds) {
            try {
                $image = Invoke-ControllerGet $nodeId "/api/v1/controller/metadata"
            }
            catch {
                return $null
            }
            if (-not (Test-TopicReady $image $script:OffsetsTopic $OffsetsTopicPartitions 3)) {
                $ready = $false
                break
            }
        }
        if ($ready) {
            return $true
        }
        $null = Invoke-CliRaw @("--broker", "invalid-admin-broker", "--controller", $controller, "topic", "delete", "--name", $script:OffsetsTopic)
        $null = Invoke-CliRaw @("--broker", "invalid-admin-broker", "--controller", $controller, "topic", "create", "--name", $script:OffsetsTopic, "--partitions", [string]$OffsetsTopicPartitions, "--replication-factor", "3")
        return $null
    } $script:TimeoutSeconds 500
}

function Show-Diagnostics {
    Write-Host ""
    Write-Host "Verification diagnostics" -ForegroundColor Yellow
    Write-Host "Artifacts: $script:WorkDir"
    Write-Host "Cluster ID: $script:ClusterId"
    foreach ($nodeId in @($script:Nodes.Keys | Sort-Object)) {
        $node = $script:Nodes[$nodeId]
        $status = "unknown"
        try {
            $node.Process.Refresh()
            $status = if ($node.Process.HasExited) { "exited code=$($node.Process.ExitCode)" } else { "running" }
        }
        catch {
            $status = "process unavailable"
        }
        Write-Host ("node {0}: pid={1} status={2}" -f $nodeId, $node.Process.Id, $status)
        foreach ($logPath in @($node.Stdout, $node.Stderr)) {
            Write-Host ("  tail " + $logPath)
            if (Test-Path -LiteralPath $logPath) {
                Get-Content -LiteralPath $logPath -Tail 100 -ErrorAction SilentlyContinue | ForEach-Object {
                    Write-Host ("    " + $_)
                }
            }
            else {
                Write-Host "    <missing>"
            }
        }
    }
    foreach ($id in @($script:Consumers.Keys | Sort-Object)) {
        $consumer = $script:Consumers[$id]
        $status = "unknown"
        try {
            $consumer.Process.Refresh()
            $status = if ($consumer.Process.HasExited) { "exited code=$($consumer.Process.ExitCode)" } else { "running" }
        }
        catch {
            $status = "process unavailable"
        }
        Write-Host ("consumer {0} (group {1}): pid={2} status={3}" -f $id, $consumer.Group, $consumer.Process.Id, $status)
        foreach ($logPath in @($consumer.Stdout, $consumer.Stderr)) {
            Write-Host ("  tail " + $logPath)
            if (Test-Path -LiteralPath $logPath) {
                Get-Content -LiteralPath $logPath -Tail 60 -ErrorAction SilentlyContinue | ForEach-Object {
                    Write-Host ("    " + $_)
                }
            }
            else {
                Write-Host "    <missing>"
            }
        }
    }
}

function Remove-VerifiedTempDirectory {
    $resolved = [IO.Path]::GetFullPath($script:WorkDir)
    $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
    $leaf = [IO.Path]::GetFileName($resolved)
    if (-not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove non-temporary path: $resolved"
    }
    if (-not $leaf.StartsWith("brahmaputra-m4-", [StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove unexpected temporary directory: $resolved"
    }
    if (Test-Path -LiteralPath $resolved) {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

try {
    if ($TimeoutSeconds -lt 20) {
        throw "TimeoutSeconds must be at least 20"
    }
    if ($HeartbeatIntervalMs -lt 50) {
        throw "HeartbeatIntervalMs must be at least 50"
    }
    if ($SessionTimeoutMs -le ($HeartbeatIntervalMs * 2)) {
        throw "SessionTimeoutMs must exceed twice HeartbeatIntervalMs"
    }
    if ($SegmentBytes -lt 4096) {
        throw "SegmentBytes must be at least 4096"
    }
    if ($OffsetsTopicPartitions -lt 1) {
        throw "OffsetsTopicPartitions must be positive"
    }
    if ($TopicPartitions -lt 3 -or ($TopicPartitions % 3) -ne 0) {
        throw "TopicPartitions must be a positive multiple of three"
    }
    if ($CommitIntervalMs -lt 100) {
        throw "CommitIntervalMs must be at least 100"
    }
    if ($CliWallTimeoutSeconds -lt 60) {
        throw "CliWallTimeoutSeconds must be at least 60"
    }

    $null = New-Item -ItemType Directory -Force -Path $script:WorkDir
    Set-Location -LiteralPath $script:ProjectRoot

    Write-Stage "Build the actual combined server and CLI binaries"
    & cargo build -p brahmaputra-server -p brahmaputra-cli
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }
    $exeSuffix = if ($env:OS -eq "Windows_NT") { ".exe" } else { "" }
    $script:ServerExe = Join-Path $script:ProjectRoot ("target\debug\brahmaputra-server" + $exeSuffix)
    $script:CliExe = Join-Path $script:ProjectRoot ("target\debug\brahmaputra-cli" + $exeSuffix)
    Assert-True (Test-Path -LiteralPath $script:ServerExe) "server binary exists"
    Assert-True (Test-Path -LiteralPath $script:CliExe) "CLI binary exists"

    foreach ($nodeId in 1..$script:NodeCount) {
        $script:NodePorts[$nodeId] = [PSCustomObject]@{
            Data = Get-FreeTcpPort
            Control = Get-FreeTcpPort
        }
    }

    Write-Stage "Launch five combined nodes and bootstrap their fixed Raft quorum"
    foreach ($nodeId in 1..$script:NodeCount) {
        $null = Start-CombinedNode $nodeId
    }
    Wait-ForHttpReady @(1, 2, 3, 4, 5)
    Pass "all five controller HTTP endpoints became ready"
    $bootstrap = Invoke-ControllerPost 1 "/api/v1/controller/bootstrap"
    $null = Assert-ControllerResultOk $bootstrap "five-member controller quorum bootstrapped through HTTP"
    $controllerLeader = Wait-ForSharedLeader @(1, 2, 3, 4, 5)
    Pass "all five controllers agreed on Raft leader $controllerLeader"

    $null = Wait-ForMetadataPredicate @(1, 2, 3, 4, 5) "all five broker registrations" {
        param($image)
        return (Test-BrokersAlive $image @(1, 2, 3, 4, 5) $true) -and (Get-MapEntries $image.brokers).Count -eq 5
    }
    Pass "all five combined nodes registered as live brokers"

    Write-Stage "Internal __consumer_offsets topic is auto-created and pinned to RF=3"
    $null = Wait-ForMetadataPredicate @(1, 2, 3, 4, 5) "server-created internal offsets topic" {
        param($image)
        return $null -ne (Get-MapValue $image.topics $script:OffsetsTopic)
    }
    Pass "the cluster auto-created $($script:OffsetsTopic) at startup"
    $metadataOutput = Invoke-Cli @("--broker", (Get-BrokerAddress 1), "metadata")
    Assert-True ($metadataOutput.Contains("topic `"$($script:OffsetsTopic)`" (error_code=0)")) "Metadata API exposes $($script:OffsetsTopic) to clients"
    Invoke-PinOffsetsTopic @(1, 2, 3, 4, 5) $controllerLeader
    Pass "$($script:OffsetsTopic) converged to $OffsetsTopicPartitions RF=3 partitions before group traffic"

    Write-Stage "Create the six-partition RF=3 group test topic"
    $createController = @(@(1..5) | Where-Object { $_ -ne $controllerLeader })[0]
    $createOutput = Invoke-Cli @(
        "--broker", "deliberately-invalid-broker-address",
        "--controller", "http://127.0.0.1:$($script:Nodes[$createController].ControlPort)",
        "topic", "create",
        "--name", $script:Topic,
        "--partitions", [string]$TopicPartitions,
        "--replication-factor", "3"
    )
    Assert-True ($createOutput.Contains("topic created name=`"$($script:Topic)`" partitions=$TopicPartitions replication_factor=3")) "CLI created the configured RF=3 test topic through a nonleader controller"
    $null = Wait-ForMetadataPredicate @(1, 2, 3, 4, 5) "six-partition assignment with full leadership" {
        param($image)
        return Test-TopicReady $image $script:Topic $script:TopicPartitions 3
    }
    Pass "all $TopicPartitions partitions have leaders and three replicas"

    Write-Stage "Three consumers in one group each own exactly two partitions"
    $runNonce = [Guid]::NewGuid().ToString("N").Substring(0, 10)
    $c1 = Start-GroupConsumer 1 $script:GroupSpread
    $c2 = Start-GroupConsumer 2 $script:GroupSpread
    $c3 = Start-GroupConsumer 3 $script:GroupSpread
    $spreadLogs = @($c1.Stdout, $c2.Stdout, $c3.Stdout)
    $null = Wait-ForValue "three-way rebalance to settle into disjoint partition pairs" {
        if (Test-AssignmentSplit $spreadLogs 2) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Pass "each of the three consumers owns exactly two partitions, disjoint and complete"

    $spreadValues = Join-Path $script:WorkDir "values-spread.txt"
    New-ValuesFile $spreadValues "spread-$runNonce" 30
    Send-ValuesFile 4 $spreadValues 30
    $null = Wait-ForValue "every spread record consumed exactly once across the group" {
        if (Test-ValuesConsumed "once" $spreadValues $spreadLogs) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Pass "all 30 records were consumed exactly once with a stable assignment"

    Write-Stage "Hard-kill one consumer mid-stream; survivors rebalance and resume its partitions"
    $preKillValues = Join-Path $script:WorkDir "values-kill-pre.txt"
    $postKillValues = Join-Path $script:WorkDir "values-kill-post.txt"
    New-ValuesFile $preKillValues "kill-pre-$runNonce" 36
    Send-ValuesFile 5 $preKillValues 36
    $null = Wait-ForValue "the group to start draining the pre-kill batch" {
        if (Test-RecordsSeen 6 $spreadLogs) { return $true }
        return $null
    } 60 200
    Stop-GroupConsumer $c2.Id
    $c2.Process.Refresh()
    Assert-True $c2.Process.HasExited "consumer $($c2.Id) was forcibly terminated mid-stream"
    New-ValuesFile $postKillValues "kill-post-$runNonce" 24
    Send-ValuesFile 1 $postKillValues 24
    Pass "produced 24 more records into the rebalancing group"
    $survivorLogs = @($c1.Stdout, $c3.Stdout)
    $null = Wait-ForValue "surviving consumers to rebalance to three partitions each" {
        if (Test-AssignmentSplit $survivorLogs 3) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Pass "dead consumer's partitions were redistributed within the session timeout"
    $allKillValues = Join-Path $script:WorkDir "values-kill-all.txt"
    $allValues = [string[]](@(Get-Content -LiteralPath $preKillValues) + @(Get-Content -LiteralPath $postKillValues))
    [IO.File]::WriteAllLines($allKillValues, $allValues)
    $killWindowLogs = @($c1.Stdout, $c2.Stdout, $c3.Stdout)
    $null = Wait-ForValue "every record consumed at least once across the group" {
        if (Test-ValuesConsumed "any" $allKillValues $killWindowLogs) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Pass "all 60 records were consumed at least once despite the mid-stream kill"
    Assert-DupsFromVictim $c2.Stdout $survivorLogs "redelivered records all come from the killed member's uncommitted tail (no rewind past committed offsets)"
    Stop-GroupConsumer $c1.Id
    Stop-GroupConsumer $c3.Id

    Write-Stage "Kill the coordinator broker; the group rejoins with committed offsets intact"
    $coordinatorPartition = Get-OffsetsPartition $script:GroupFailover
    $coordinator = Get-PartitionLeader $controllerLeader $script:OffsetsTopic $coordinatorPartition
    Assert-True ($null -ne $coordinator -and $coordinator -ge 1) "group $($script:GroupFailover) maps to $($script:OffsetsTopic)-$coordinatorPartition led by broker $coordinator"
    $f1 = Start-GroupConsumer $coordinator $script:GroupFailover
    $null = Wait-ForValue "single consumer to hold all six partitions" {
        if (Test-AssignmentSplit @($f1.Stdout) 6) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Pass "group stabilized on coordinator broker $coordinator"
    $failoverValues = Join-Path $script:WorkDir "values-failover.txt"
    New-ValuesFile $failoverValues "coord-$runNonce" 18
    Send-ValuesFile 2 $failoverValues 18
    $null = Wait-ForValue "pre-failover records consumed exactly once" {
        if (Test-ValuesConsumed "once" $failoverValues @($f1.Stdout)) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Start-Sleep -Seconds 2 # at least two auto-commit intervals, so every position is committed
    Pass "consumer positions were committed to $($script:OffsetsTopic) before the failover"
    Stop-GroupConsumer $f1.Id
    Stop-CombinedNode $coordinator -Force
    $script:Nodes[$coordinator].Process.Refresh()
    Assert-True $script:Nodes[$coordinator].Process.HasExited "coordinator broker $coordinator was forcibly terminated"
    $liveNodes = @(@(1..5) | Where-Object { $_ -ne $coordinator })
    $null = Wait-ForSharedLeader $liveNodes
    $null = Wait-ForMetadataPredicate $liveNodes "offsets partition failover away from broker $coordinator" {
        param($image)
        $partition = Get-MapValue (Get-MapValue $image.topics $script:OffsetsTopic).partitions $coordinatorPartition
        $dead = Get-MapValue $image.brokers $coordinator
        return $null -ne $partition -and $null -ne $dead -and -not [bool]$dead.alive -and
            [int]$partition.leader -ge 0 -and [int]$partition.leader -ne $coordinator
    }
    Pass "controller moved $($script:OffsetsTopic)-$coordinatorPartition to a surviving broker"
    $f2 = Start-GroupConsumer ([int]$liveNodes[0]) $script:GroupFailover
    $null = Wait-ForValue "fresh consumer to rejoin and take all six partitions on the new coordinator" {
        if (Test-AssignmentSplit @($f2.Stdout) 6) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Pass "group rejoined on the new coordinator after the old member's session expired"
    $postFailoverValues = Join-Path $script:WorkDir "values-post-failover.txt"
    New-ValuesFile $postFailoverValues "coord-post-$runNonce" 6
    Send-ValuesFile 3 $postFailoverValues 6
    $null = Wait-ForValue "post-failover records consumed exactly once" {
        if (Test-ValuesConsumed "once" $postFailoverValues @($f2.Stdout)) { return $true }
        return $null
    } $script:TimeoutSeconds 500
    Assert-ValuesAbsent $failoverValues $f2.Stdout "consumption resumed from committed offsets, not earliest (no pre-failover redelivery)"
    Stop-GroupConsumer $f2.Id

    Write-Stage "A bounded consume run resumes from the previous run's committed offsets"
    # Dedicated topic: the shared test topic carries records from every
    # prior scenario, and a brand-new group reading from earliest must see
    # only the records this scenario produces.
    $resumeTopic = "$($script:Topic)-resume"
    $null = Invoke-Cli @(
        "--broker", "deliberately-invalid-broker-address",
        "--controller", "http://127.0.0.1:$($script:Nodes[$createController].ControlPort)",
        "topic", "create",
        "--name", $resumeTopic,
        "--partitions", [string]$TopicPartitions,
        "--replication-factor", "3"
    )
    $null = Wait-ForMetadataPredicate @(2, 3, 4, 5) "resume topic assignment with full leadership" {
        param($image)
        return Test-TopicReady $image $resumeTopic $script:TopicPartitions 3
    }
    $resumeFirstValues = Join-Path $script:WorkDir "values-resume-first.txt"
    $resumeSecondValues = Join-Path $script:WorkDir "values-resume-second.txt"
    New-ValuesFile $resumeFirstValues "resume-a-$runNonce" 12
    Send-ValuesFile 4 $resumeFirstValues 12 $resumeTopic
    $firstRun = Invoke-Cli @(
        "--broker", (Get-BrokerAddress 5),
        "consume", "--topic", $resumeTopic,
        "--group", $script:GroupResume,
        "--max", "12",
        "--commit-interval-ms", [string]$CommitIntervalMs
    )
    Assert-ResumeRun $firstRun $resumeFirstValues 0 "first bounded run consumed and committed the initial 12 records"
    New-ValuesFile $resumeSecondValues "resume-b-$runNonce" 6
    Send-ValuesFile 2 $resumeSecondValues 6 $resumeTopic
    $secondRun = Invoke-Cli @(
        "--broker", (Get-BrokerAddress 2),
        "consume", "--topic", $resumeTopic,
        "--group", $script:GroupResume,
        "--max", "100",
        "--commit-interval-ms", [string]$CommitIntervalMs
    )
    Assert-ResumeRun $secondRun $resumeSecondValues 2 "second run resumed at the committed position (offset 2 per partition) and consumed only the 6 new records"

    Write-Stage "M4 live verification complete"
    Write-Host ("offsets topic: {0} partitions (RF=3), group coordinator for {1}: {2}-{3}" -f $OffsetsTopicPartitions, $script:GroupFailover, $script:OffsetsTopic, $coordinatorPartition)
    Write-Host ("checks passed: " + $script:Checks)
    $script:Succeeded = $true
}
catch {
    Write-Host ""
    Write-Host ("FAIL: " + $_.Exception.Message) -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor DarkRed
    Show-Diagnostics
}
finally {
    foreach ($id in @($script:Consumers.Keys)) {
        Stop-GroupConsumer ([int]$id)
    }
    foreach ($nodeId in @($script:Nodes.Keys)) {
        Stop-CombinedNode ([int]$nodeId) -Force
    }
    if ($script:Succeeded) {
        try {
            Remove-VerifiedTempDirectory
        }
        catch {
            Write-Warning $_.Exception.Message
        }
    }
    else {
        Write-Host ("Verification artifacts retained at: " + $script:WorkDir) -ForegroundColor Yellow
    }
}

if (-not $script:Succeeded) {
    exit 1
}
