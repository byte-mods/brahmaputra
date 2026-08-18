# M3 live verification: replicated commit, ISR failover, leader-epoch
# truncation, extended catch-up, on-disk prefix identity, and min ISR.
#
# The cluster has five combined nodes. The test topic has one RF=3 partition,
# so two non-replica nodes keep the five-member controller quorum writable
# while two of the partition's three brokers are hard-killed.
#
# This script targets Windows PowerShell 5 as well as newer PowerShell:
#
#   powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\verify-m3.ps1

[CmdletBinding()]
param(
    [int]$TimeoutSeconds = 120,
    [int]$HeartbeatIntervalMs = 500,
    [int]$SessionTimeoutMs = 5000,
    [int]$StormProcesses = 64,
    [int]$ExtendedDowntimeSeconds = 600,
    [int]$ExtendedProduceIntervalMs = 1000,
    [int]$ExtendedBacklogRecords = 1024,
    [int]$ExtendedValueBytes = 32768,
    [int]$SegmentBytes = 1048576
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

$script:Checks = 0
$script:Succeeded = $false
$script:NodeCount = 5
$script:Nodes = @{}
$script:AllocatedPorts = New-Object 'System.Collections.Generic.HashSet[int]'
$script:ProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$script:WorkDir = Join-Path ([IO.Path]::GetTempPath()) ("brahmaputra-m3-" + [Guid]::NewGuid().ToString("N"))
$script:ClusterId = "m3-live-" + [Guid]::NewGuid().ToString("N").Substring(0, 12)
$script:Topic = "replication-live"
$script:Partition = 0
$script:ServerExe = $null
$script:CliExe = $null
$script:NodePorts = @{}
$script:CaptureSequence = 0

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

function Get-MapEntries {
    param([object]$Map)
    if ($null -eq $Map) {
        return @()
    }
    return @($Map.PSObject.Properties)
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
        [string]$Path,
        [AllowNull()][object]$Body = $null
    )
    $node = $script:Nodes[$NodeId]
    $uri = "http://127.0.0.1:$($node.ControlPort)$Path"
    if ($null -eq $Body) {
        return Invoke-RestMethod -Uri $uri -Method Post -TimeoutSec 20
    }
    $json = $Body | ConvertTo-Json -Depth 20 -Compress
    return Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json" -Body $json -TimeoutSec 20
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
        "--session-timeout-ms", [string]$SessionTimeoutMs
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
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
                $null = $process.WaitForExit(3000)
            }
        }
    }
    catch {
        Write-Warning ("could not stop node {0} pid {1}: {2}" -f $NodeId, $process.Id, $_.Exception.Message)
    }
}

function Get-LiveNodeIds {
    $live = @()
    foreach ($nodeId in @($script:Nodes.Keys | Sort-Object)) {
        $process = $script:Nodes[$nodeId].Process
        $process.Refresh()
        if (-not $process.HasExited) {
            $live += [int]$nodeId
        }
    }
    return $live
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

function Get-TopicPartition {
    param([object]$Image)
    $topic = Get-MapValue $Image.topics $script:Topic
    if ($null -eq $topic) {
        return $null
    }
    return Get-MapValue $topic.partitions $script:Partition
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

function Invoke-AcksAllProduce {
    param(
        [int]$SeedNodeId,
        [string]$Value,
        [int]$TimeoutMs = 30000
    )
    return Invoke-Cli @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "produce", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--value", $Value,
        "--acks", "all",
        "--timeout-ms", [string]$TimeoutMs
    )
}

function Get-Offsets {
    param([int]$SeedNodeId)
    $output = Invoke-Cli @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "offsets", "--topic", $script:Topic,
        "--partition", [string]$script:Partition
    )
    $match = [Regex]::Match($output, "(?m)^$([Regex]::Escape($script:Topic))-$($script:Partition): earliest=([0-9]+) latest=([0-9]+)$")
    if (-not $match.Success) {
        throw "cannot parse offsets output: $output"
    }
    return [PSCustomObject]@{
        Earliest = [Int64]$match.Groups[1].Value
        Latest = [Int64]$match.Groups[2].Value
    }
}

function Get-OneRecord {
    param(
        [int]$SeedNodeId,
        [Int64]$Offset
    )
    $output = Invoke-Cli @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "consume", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--offset", [string]$Offset,
        "--max", "1"
    )
    $match = [Regex]::Match($output, '(?m)^partition=([0-9]+) offset=([0-9]+) key=(.*?) value=(.*)$')
    if (-not $match.Success) {
        throw "cannot parse consumed record at offset $Offset`: $output"
    }
    return [PSCustomObject]@{
        Partition = [int]$match.Groups[1].Value
        Offset = [Int64]$match.Groups[2].Value
        Key = $match.Groups[3].Value
        Value = $match.Groups[4].Value
    }
}

function Start-CapturedProduce {
    param(
        [int]$SeedNodeId,
        [string]$Value,
        [string]$Label
    )
    $script:CaptureSequence++
    $captureDir = Join-Path $script:WorkDir "captures"
    $null = New-Item -ItemType Directory -Force -Path $captureDir
    $stem = "{0}-{1:D4}" -f $Label, $script:CaptureSequence
    $stdout = Join-Path $captureDir ($stem + ".stdout.log")
    $stderr = Join-Path $captureDir ($stem + ".stderr.log")
    $arguments = @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "produce", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--value", $Value,
        "--acks", "all",
        "--timeout-ms", "30000"
    )
    $argumentLine = (($arguments | ForEach-Object { Quote-ProcessArgument ([string]$_) }) -join " ")
    $process = Start-Process -FilePath $script:CliExe `
        -ArgumentList $argumentLine `
        -WorkingDirectory $script:ProjectRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru
    return [PSCustomObject]@{
        Process = $process
        Stdout = $stdout
        Stderr = $stderr
        Value = $Value
        Arguments = $argumentLine
    }
}

function Get-CapturedText {
    param([object]$Capture)
    $lines = @()
    foreach ($path in @($Capture.Stdout, $Capture.Stderr)) {
        if (Test-Path -LiteralPath $path) {
            $lines += @(Get-Content -LiteralPath $path -ErrorAction SilentlyContinue)
        }
    }
    return (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
}

function Start-ProduceStorm {
    param(
        [int]$SeedNodeId,
        [string]$Label
    )
    $captures = @()
    $nonce = [Guid]::NewGuid().ToString("N").Substring(0, 10)
    for ($i = 0; $i -lt $StormProcesses; $i++) {
        $value = "$Label-$nonce-$('{0:D4}' -f $i)"
        $captures += ,(Start-CapturedProduce $SeedNodeId $value $Label)
    }
    return $captures
}

function Wait-ForStormArmed {
    param([object[]]$Captures)
    return Wait-ForValue "produce storm to have acknowledged and in-flight calls" {
        $acked = 0
        $running = 0
        foreach ($capture in $Captures) {
            $capture.Process.Refresh()
            if (-not $capture.Process.HasExited) {
                $running++
                continue
            }
            if ($capture.Process.ExitCode -eq 0 -and (Get-CapturedText $capture) -match '(?m)^acked offset=([0-9]+)$') {
                $acked++
            }
        }
        if ($acked -ge 3 -and $running -ge 2) {
            return [PSCustomObject]@{ Acked = $acked; Running = $running }
        }
        return $null
    } 30 20
}

function Complete-ProduceStorm {
    param([object[]]$Captures)
    foreach ($capture in $Captures) {
        $capture.Process.Refresh()
        if (-not $capture.Process.HasExited) {
            $null = $capture.Process.WaitForExit(45000)
            $capture.Process.Refresh()
        }
        if (-not $capture.Process.HasExited) {
            Stop-Process -Id $capture.Process.Id -Force -ErrorAction SilentlyContinue
            $null = $capture.Process.WaitForExit(3000)
        }
    }

    $acked = @()
    $failures = 0
    foreach ($capture in $Captures) {
        $capture.Process.Refresh()
        $text = Get-CapturedText $capture
        $match = [Regex]::Match($text, '(?m)^acked offset=([0-9]+)$')
        if ($capture.Process.ExitCode -eq 0 -and $match.Success) {
            $acked += ,[PSCustomObject]@{
                Offset = [Int64]$match.Groups[1].Value
                Value = $capture.Value
            }
        }
        else {
            $failures++
        }
    }
    Write-Host ("storm outcomes: acknowledged={0}, failed-or-ambiguous={1}" -f $acked.Count, $failures)
    Assert-True ($acked.Count -ge 3) "produce storm retained at least three explicit acks across the hard kill"
    return $acked
}

function Assert-AcknowledgedContinuity {
    param(
        [int]$SeedNodeId,
        [object[]]$Acknowledged,
        [string]$Description
    )
    $offsets = Get-Offsets $SeedNodeId
    Assert-True ($offsets.Earliest -eq 0) "$Description retains offset zero"
    Assert-True ($offsets.Latest -gt 0) "$Description exposes a nonempty committed prefix"
    $output = Invoke-Cli @(
        "--broker", (Get-BrokerAddress $SeedNodeId),
        "consume", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--from", "earliest",
        "--max", [string]$offsets.Latest
    )
    $records = @()
    foreach ($line in @($output -split "`r?`n")) {
        $match = [Regex]::Match($line, '^partition=([0-9]+) offset=([0-9]+) key=(.*?) value=(.*)$')
        if ($match.Success) {
            $records += ,[PSCustomObject]@{
                Offset = [Int64]$match.Groups[2].Value
                Value = $match.Groups[4].Value
            }
        }
    }
    Assert-True ($records.Count -eq $offsets.Latest) "$Description consumer returned exactly every committed record"
    for ([Int64]$expected = 0; $expected -lt $offsets.Latest; $expected++) {
        if ($records[[int]$expected].Offset -ne $expected) {
            throw "assertion failed: $Description has a gap or duplicate at expected offset $expected (saw $($records[[int]$expected].Offset))"
        }
    }
    Pass "$Description offsets are contiguous with no gaps or duplicates"

    $valueCount = @{}
    foreach ($record in $records) {
        if (-not $valueCount.ContainsKey($record.Value)) {
            $valueCount[$record.Value] = 0
        }
        $valueCount[$record.Value]++
    }
    foreach ($ack in $Acknowledged) {
        if ($ack.Offset -ge $offsets.Latest) {
            throw "assertion failed: acknowledged offset $($ack.Offset) is beyond committed latest $($offsets.Latest)"
        }
        $record = $records[[int]$ack.Offset]
        if ($record.Value -ne $ack.Value) {
            throw "assertion failed: acknowledged offset $($ack.Offset) expected value $($ack.Value), saw $($record.Value)"
        }
        if ([int]$valueCount[$ack.Value] -ne 1) {
            throw "assertion failed: acknowledged value $($ack.Value) appeared $($valueCount[$ack.Value]) times"
        }
    }
    Pass "$Description contains every explicitly acknowledged value exactly once at its acked offset"
    return $offsets.Latest
}

function Read-Int32BigEndian {
    param(
        [byte[]]$Bytes,
        [int]$Offset
    )
    return [Net.IPAddress]::NetworkToHostOrder([BitConverter]::ToInt32($Bytes, $Offset))
}

function Read-Int64BigEndian {
    param(
        [byte[]]$Bytes,
        [int]$Offset
    )
    return [Net.IPAddress]::NetworkToHostOrder([BitConverter]::ToInt64($Bytes, $Offset))
}

function Read-UInt32BigEndian {
    param(
        [byte[]]$Bytes,
        [int]$Offset
    )
    $copy = New-Object byte[] 4
    [Array]::Copy($Bytes, $Offset, $copy, 0, 4)
    if ([BitConverter]::IsLittleEndian) {
        [Array]::Reverse($copy)
    }
    return [BitConverter]::ToUInt32($copy, 0)
}

function Read-UInt64BigEndian {
    param(
        [byte[]]$Bytes,
        [int]$Offset
    )
    $copy = New-Object byte[] 8
    [Array]::Copy($Bytes, $Offset, $copy, 0, 8)
    if ([BitConverter]::IsLittleEndian) {
        [Array]::Reverse($copy)
    }
    return [BitConverter]::ToUInt64($copy, 0)
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

function Read-ExactBytes {
    param(
        [IO.BinaryReader]$Reader,
        [int]$Count,
        [string]$Description
    )
    $bytes = $Reader.ReadBytes($Count)
    if ($bytes.Length -ne $Count) {
        throw "truncated $Description`: needed $Count bytes, got $($bytes.Length)"
    }
    return ,$bytes
}

function Get-PartitionDir {
    param([int]$NodeId)
    return Join-Path $script:Nodes[$NodeId].DataDir ("{0}-{1}" -f $script:Topic, $script:Partition)
}

function Read-HighWatermark {
    param([int]$NodeId)
    $path = Join-Path (Get-PartitionDir $NodeId) "hwm"
    if (-not (Test-Path -LiteralPath $path)) {
        return [Int64]0
    }
    $bytes = [IO.File]::ReadAllBytes($path)
    if ($bytes.Length -lt 8) {
        return [Int64]0
    }
    [Int64]$highWatermark = Read-Int64BigEndian $bytes 0
    if ($highWatermark -lt 0) {
        return [Int64]0
    }
    [UInt64]$generation = 0
    $offset = 8
    while (($bytes.Length - $offset) -ge 36) {
        $record = New-Object byte[] 36
        [Array]::Copy($bytes, $offset, $record, 0, 36)
        if ($record[0] -ne 72 -or $record[1] -ne 87 -or $record[2] -ne 77 -or $record[3] -ne 74 -or
            $record[4] -ne 1 -or $record[6] -ne 0 -or $record[7] -ne 0) {
            break
        }
        [byte]$kind = $record[5]
        if ($kind -ne 0 -and $kind -ne 1) {
            break
        }
        [UInt32]$expectedChecksum = Read-UInt32BigEndian $record 32
        [UInt32]$actualChecksum = Get-Crc32C -Bytes $record -Offset 0 -Count 32
        if ($actualChecksum -ne $expectedChecksum) {
            break
        }
        [UInt64]$nextGeneration = Read-UInt64BigEndian $record 8
        [Int64]$previousHighWatermark = Read-Int64BigEndian $record 16
        [Int64]$nextHighWatermark = Read-Int64BigEndian $record 24
        if ($generation -eq [UInt64]::MaxValue -or
            $nextGeneration -ne [UInt64]($generation + 1) -or
            $previousHighWatermark -ne $highWatermark -or $nextHighWatermark -lt 0) {
            break
        }
        if (($kind -eq 0 -and $nextHighWatermark -lt $highWatermark) -or
            ($kind -eq 1 -and $nextHighWatermark -gt $highWatermark)) {
            break
        }
        $generation = $nextGeneration
        $highWatermark = $nextHighWatermark
        $offset += 36
    }
    return $highWatermark
}

function Get-LogState {
    param([int]$NodeId)
    $partitionDir = Get-PartitionDir $NodeId
    if (-not (Test-Path -LiteralPath $partitionDir)) {
        return [PSCustomObject]@{
            HighWatermark = [Int64]0
            LogEnd = [Int64]0
            BatchCount = 0
            LastBatch = $null
        }
    }
    $files = @(Get-ChildItem -LiteralPath $partitionDir -Filter "*.log" | Sort-Object Name)
    [Int64]$expectedOffset = 0
    $batchCount = 0
    $lastBatch = $null
    foreach ($file in $files) {
        $stream = [IO.File]::Open($file.FullName, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
        $reader = [IO.BinaryReader]::new($stream)
        try {
            while ($stream.Position -lt $stream.Length) {
                $header = Read-ExactBytes $reader 12 "batch header in $($file.FullName)"
                [Int64]$baseOffset = Read-Int64BigEndian $header 0
                $batchLength = Read-Int32BigEndian $header 8
                if ($batchLength -lt 23) {
                    throw "invalid batch length $batchLength in $($file.FullName)"
                }
                $body = Read-ExactBytes $reader $batchLength "batch body in $($file.FullName)"
                $lastDelta = Read-Int32BigEndian $body 11
                if ($baseOffset -ne $expectedOffset -or $lastDelta -lt 0) {
                    throw "non-contiguous on-disk batch in $($file.FullName): expected=$expectedOffset base=$baseOffset delta=$lastDelta"
                }
                $raw = New-Object byte[] (12 + $batchLength)
                [Array]::Copy($header, 0, $raw, 0, 12)
                [Array]::Copy($body, 0, $raw, 12, $batchLength)
                $expectedOffset = $baseOffset + [Int64]$lastDelta + 1
                $batchCount++
                $lastBatch = $raw
            }
        }
        finally {
            $reader.Dispose()
            $stream.Dispose()
        }
    }
    return [PSCustomObject]@{
        HighWatermark = [Int64](Read-HighWatermark $NodeId)
        LogEnd = $expectedOffset
        BatchCount = $batchCount
        LastBatch = $lastBatch
    }
}

function Get-BytesSha256 {
    param([byte[]]$Bytes)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace("-", "")
    }
    finally {
        $sha.Dispose()
    }
}

function Get-BatchAtOffset {
    param(
        [int]$NodeId,
        [Int64]$TargetOffset
    )
    $partitionDir = Get-PartitionDir $NodeId
    foreach ($file in @(Get-ChildItem -LiteralPath $partitionDir -Filter "*.log" | Sort-Object Name)) {
        $stream = [IO.File]::Open($file.FullName, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
        $reader = [IO.BinaryReader]::new($stream)
        try {
            while ($stream.Position -lt $stream.Length) {
                $header = Read-ExactBytes $reader 12 "batch header in $($file.FullName)"
                [Int64]$baseOffset = Read-Int64BigEndian $header 0
                $batchLength = Read-Int32BigEndian $header 8
                $body = Read-ExactBytes $reader $batchLength "batch body in $($file.FullName)"
                $lastDelta = Read-Int32BigEndian $body 11
                $batchEnd = $baseOffset + [Int64]$lastDelta + 1
                if ($TargetOffset -ge $baseOffset -and $TargetOffset -lt $batchEnd) {
                    $raw = New-Object byte[] (12 + $batchLength)
                    [Array]::Copy($header, 0, $raw, 0, 12)
                    [Array]::Copy($body, 0, $raw, 12, $batchLength)
                    return ,$raw
                }
            }
        }
        finally {
            $reader.Dispose()
            $stream.Dispose()
        }
    }
    return $null
}

function Inject-DivergentTail {
    param([int]$NodeId)
    $state = Get-LogState $NodeId
    if ($state.LogEnd -le 0 -or $null -eq $state.LastBatch) {
        throw "node $NodeId has no batch to clone for divergent-tail injection"
    }
    if ($state.HighWatermark -gt $state.LogEnd) {
        throw "node $NodeId has an invalid stopped log state before injection (hwm=$($state.HighWatermark), leo=$($state.LogEnd))"
    }
    [byte[]]$injected = $state.LastBatch.Clone()
    $networkOffset = [Net.IPAddress]::HostToNetworkOrder([Int64]$state.LogEnd)
    $offsetBytes = [BitConverter]::GetBytes($networkOffset)
    [Array]::Copy($offsetBytes, 0, $injected, 0, 8)

    $partitionDir = Get-PartitionDir $NodeId
    $active = @(Get-ChildItem -LiteralPath $partitionDir -Filter "*.log" | Sort-Object Name)[-1]
    $stream = [IO.File]::Open($active.FullName, [IO.FileMode]::Append, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    try {
        $stream.Write($injected, 0, $injected.Length)
        $stream.Flush()
    }
    finally {
        $stream.Dispose()
    }
    $after = Get-LogState $NodeId
    if ($after.LogEnd -le $state.LogEnd) {
        throw "divergent-tail injection did not advance node $NodeId log end"
    }
    return [PSCustomObject]@{
        BaseOffset = [Int64]$state.LogEnd
        EndOffset = [Int64]$after.LogEnd
        Hash = Get-BytesSha256 $injected
    }
}

function Get-CommittedDigest {
    param(
        [int]$NodeId,
        [Int64]$HighWatermark
    )
    $sha = [Security.Cryptography.SHA256]::Create()
    [Int64]$expectedOffset = 0
    [Int64]$hashedBytes = 0
    $batchCount = 0
    try {
        $partitionDir = Get-PartitionDir $NodeId
        foreach ($file in @(Get-ChildItem -LiteralPath $partitionDir -Filter "*.log" | Sort-Object Name)) {
            $stream = [IO.File]::Open($file.FullName, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
            $reader = [IO.BinaryReader]::new($stream)
            try {
                while ($stream.Position -lt $stream.Length -and $expectedOffset -lt $HighWatermark) {
                    $header = Read-ExactBytes $reader 12 "batch header in $($file.FullName)"
                    [Int64]$baseOffset = Read-Int64BigEndian $header 0
                    $batchLength = Read-Int32BigEndian $header 8
                    $body = Read-ExactBytes $reader $batchLength "batch body in $($file.FullName)"
                    $lastDelta = Read-Int32BigEndian $body 11
                    $batchEnd = $baseOffset + [Int64]$lastDelta + 1
                    if ($baseOffset -ne $expectedOffset) {
                        throw "node $NodeId committed image is non-contiguous: expected=$expectedOffset base=$baseOffset"
                    }
                    if ($batchEnd -gt $HighWatermark) {
                        throw "cluster HWM $HighWatermark splits batch [$baseOffset,$batchEnd) on node $NodeId"
                    }
                    $null = $sha.TransformBlock($header, 0, $header.Length, $header, 0)
                    $null = $sha.TransformBlock($body, 0, $body.Length, $body, 0)
                    $hashedBytes += $header.Length + $body.Length
                    $batchCount++
                    $expectedOffset = $batchEnd
                }
            }
            finally {
                $reader.Dispose()
                $stream.Dispose()
            }
            if ($expectedOffset -eq $HighWatermark) {
                break
            }
        }
        if ($expectedOffset -ne $HighWatermark) {
            throw "node $NodeId committed image ended at $expectedOffset, expected HWM $HighWatermark"
        }
        $null = $sha.TransformFinalBlock((New-Object byte[] 0), 0, 0)
        return [PSCustomObject]@{
            NodeId = $NodeId
            HighWatermark = $HighWatermark
            Hash = ([BitConverter]::ToString($sha.Hash)).Replace("-", "")
            Bytes = $hashedBytes
            Batches = $batchCount
        }
    }
    finally {
        $sha.Dispose()
    }
}

function Assert-ReplicaPrefixIdentity {
    param(
        [int[]]$ReplicaIds,
        [string]$Description
    )
    $states = @()
    foreach ($replicaId in $ReplicaIds) {
        $states += ,(Get-LogState $replicaId)
    }
    [Int64]$clusterHwm = ($states | ForEach-Object { [Int64]$_.HighWatermark } | Measure-Object -Minimum).Minimum
    Assert-True ($clusterHwm -gt 0) "$Description has a positive cluster-wide persisted HWM"
    $digests = @()
    foreach ($replicaId in $ReplicaIds) {
        $digests += ,(Get-CommittedDigest $replicaId $clusterHwm)
    }
    $hashes = @($digests | ForEach-Object { $_.Hash } | Select-Object -Unique)
    $bytes = @($digests | ForEach-Object { $_.Bytes } | Select-Object -Unique)
    $batches = @($digests | ForEach-Object { $_.Batches } | Select-Object -Unique)
    Assert-True ($hashes.Count -eq 1 -and $bytes.Count -eq 1 -and $batches.Count -eq 1) "$Description is byte-identical on every assigned replica through HWM $clusterHwm"
    Write-Host ("committed prefix: hwm={0} batches={1} bytes={2} sha256={3}" -f $clusterHwm, $batches[0], $bytes[0], $hashes[0])
    return $digests[0]
}

function New-IncompressibleRecordFile {
    param(
        [string]$Path,
        [int]$RecordCount,
        [int]$ValueLength
    )
    $encoding = [Text.UTF8Encoding]::new($false)
    $writer = [IO.StreamWriter]::new($Path, $false, $encoding)
    $random = [Security.Cryptography.RandomNumberGenerator]::Create()
    $rawLength = [Math]::Max(1, [int][Math]::Ceiling($ValueLength * 0.75))
    $buffer = New-Object byte[] $rawLength
    try {
        for ($index = 0; $index -lt $RecordCount; $index++) {
            $random.GetBytes($buffer)
            $line = [Convert]::ToBase64String($buffer)
            if ($line.Length -gt $ValueLength) {
                $line = $line.Substring(0, $ValueLength)
            }
            elseif ($line.Length -lt $ValueLength) {
                $line = $line.PadRight($ValueLength, 'z')
            }
            $writer.WriteLine($line)
        }
    }
    finally {
        $writer.Dispose()
        $random.Dispose()
    }
}

function Wait-ForFullIsr {
    param(
        [int[]]$NodeIds,
        [int[]]$ReplicaIds,
        [string]$Description
    )
    return Wait-ForMetadataPredicate $NodeIds $Description {
        param($image)
        $partition = Get-TopicPartition $image
        if ($null -eq $partition) { return $false }
        $actual = @($partition.isr | ForEach-Object { [int]$_ } | Sort-Object)
        $expected = @($ReplicaIds | Sort-Object)
        return (($actual -join ',') -eq ($expected -join ',')) -and ($expected -contains [int]$partition.leader)
    }
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
        Write-Host ("  args: " + $node.Arguments)
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
}

function Remove-VerifiedTempDirectory {
    $resolved = [IO.Path]::GetFullPath($script:WorkDir)
    $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
    $leaf = [IO.Path]::GetFileName($resolved)
    if (-not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove non-temporary path: $resolved"
    }
    if (-not $leaf.StartsWith("brahmaputra-m3-", [StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove unexpected temporary directory: $resolved"
    }
    if (Test-Path -LiteralPath $resolved) {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

try {
    if ($HeartbeatIntervalMs -lt 50) {
        throw "HeartbeatIntervalMs must be at least 50"
    }
    if ($SessionTimeoutMs -le ($HeartbeatIntervalMs * 2)) {
        throw "SessionTimeoutMs must exceed twice HeartbeatIntervalMs"
    }
    if ($StormProcesses -lt 8) {
        throw "StormProcesses must be at least 8"
    }
    if ($ExtendedDowntimeSeconds -lt 1) {
        throw "ExtendedDowntimeSeconds must be at least 1"
    }
    if ($ExtendedProduceIntervalMs -lt 50) {
        throw "ExtendedProduceIntervalMs must be at least 50"
    }
    if ($ExtendedBacklogRecords -lt 1 -or $ExtendedValueBytes -lt 1) {
        throw "extended backlog record count and value size must be positive"
    }
    if ($SegmentBytes -lt 4096) {
        throw "SegmentBytes must be at least 4096"
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

    $initialImage = Wait-ForMetadataPredicate @(1, 2, 3, 4, 5) "all five broker registrations" {
        param($image)
        return (Test-BrokersAlive $image @(1, 2, 3, 4, 5) $true) -and (Get-MapEntries $image.brokers).Count -eq 5
    }
    Pass "all five combined nodes registered as live brokers"

    Write-Stage "Create one RF=3 partition with min.insync.replicas=2 through the CLI"
    $createController = @(1..5 | Where-Object { $_ -ne $controllerLeader })[0]
    $createOutput = Invoke-Cli @(
        "--broker", "deliberately-invalid-broker-address",
        "--controller", "http://127.0.0.1:$($script:Nodes[$createController].ControlPort)",
        "topic", "create",
        "--name", $script:Topic,
        "--partitions", "1",
        "--replication-factor", "3",
        "--config", "min.insync.replicas=2"
    )
    Assert-True ($createOutput.Contains("topic created name=`"$($script:Topic)`" partitions=1 replication_factor=3")) "CLI created the configured RF=3 topic through a nonleader controller"
    $topicImage = Wait-ForMetadataPredicate @(1, 2, 3, 4, 5) "RF=3 assignment and initial full ISR" {
        param($image)
        $partition = Get-TopicPartition $image
        if ($null -eq $partition) { return $false }
        return @($partition.replicas).Count -eq 3 -and @($partition.isr).Count -eq 3
    }
    $initialPartition = Get-TopicPartition $topicImage
    $replicas = @($initialPartition.replicas | ForEach-Object { [int]$_ })
    $witnesses = @(1..5 | Where-Object { $replicas -notcontains $_ })
    Assert-True ($replicas.Count -eq 3 -and $witnesses.Count -eq 2) "partition has exactly three assigned replicas and two non-replica controller witnesses"
    Assert-True ((@($replicas | Sort-Object) -join ',') -eq '1,2,3') "deterministic placement selected brokers 1,2,3 for the verification partition"

    Write-Stage "RF=3 acks=all storm: hard-kill the leader with requests in flight"
    $firstLeader = [int]$initialPartition.leader
    $firstSeed = [int]$witnesses[0]
    $firstStorm = Start-ProduceStorm $firstSeed "storm-one"
    $armed = Wait-ForStormArmed $firstStorm
    Pass "first storm had $($armed.Acked) acknowledged and $($armed.Running) in-flight calls at the kill point"
    Stop-CombinedNode $firstLeader -Force
    $script:Nodes[$firstLeader].Process.Refresh()
    Assert-True $script:Nodes[$firstLeader].Process.HasExited "partition leader $firstLeader was forcibly terminated mid-storm"

    $liveAfterFirstKill = @(1..5 | Where-Object { $_ -ne $firstLeader })
    $null = Wait-ForSharedLeader $liveAfterFirstKill
    $firstFailoverImage = Wait-ForMetadataPredicate $liveAfterFirstKill "first leader fencing and ISR failover" {
        param($image)
        $partition = Get-TopicPartition $image
        $dead = Get-MapValue $image.brokers $firstLeader
        return $null -ne $partition -and $null -ne $dead -and -not [bool]$dead.alive -and
            [int]$partition.leader -ne $firstLeader -and @($partition.isr).Count -eq 2 -and
            -not (@($partition.isr) -contains $firstLeader) -and (@($partition.isr) -contains [int]$partition.leader)
    }
    Pass "controller elected a new leader from the surviving ISR and removed the killed broker"
    $firstAcknowledged = Complete-ProduceStorm $firstStorm
    $firstNewLeader = [int](Get-TopicPartition $firstFailoverImage).leader
    $firstLatest = Assert-AcknowledgedContinuity ([int]$witnesses[1]) $firstAcknowledged "first leader failover"
    Pass "first acks=all failover preserved a committed prefix through latest offset $firstLatest"

    $null = Start-CombinedNode $firstLeader -Restart
    Wait-ForHttpReady @($firstLeader)
    $fullAfterFirst = Wait-ForFullIsr @(1, 2, 3, 4, 5) $replicas "killed first leader to catch up and re-enter ISR"
    Assert-True ([int](Get-TopicPartition $fullAfterFirst).leader -eq $firstNewLeader) "first killed leader rejoined as a follower without stealing leadership"

    Write-Stage "Follower-stall case: kill the leader during ISR=2 acks=all traffic, then prove epoch truncation"
    $secondPartition = Get-TopicPartition $fullAfterFirst
    $secondLeader = [int]$secondPartition.leader
    $stalledFollower = @($replicas | Where-Object { $_ -ne $secondLeader })[0]
    $survivingFollower = @($replicas | Where-Object { $_ -ne $secondLeader -and $_ -ne $stalledFollower })[0]
    Stop-CombinedNode $stalledFollower -Force
    Assert-True $script:Nodes[$stalledFollower].Process.HasExited "follower $stalledFollower was hard-stalled by process termination"
    $liveWithoutStalled = @(1..5 | Where-Object { $_ -ne $stalledFollower })
    $isrTwoImage = Wait-ForMetadataPredicate $liveWithoutStalled "stalled follower fencing and ISR shrink to two" {
        param($image)
        $partition = Get-TopicPartition $image
        $broker = Get-MapValue $image.brokers $stalledFollower
        return $null -ne $partition -and $null -ne $broker -and -not [bool]$broker.alive -and
            @($partition.isr).Count -eq 2 -and -not (@($partition.isr) -contains $stalledFollower)
    }
    Pass "stalled follower left ISR before the second failure"

    $secondStorm = Start-ProduceStorm ([int]$witnesses[0]) "storm-two"
    $secondArmed = Wait-ForStormArmed $secondStorm
    Pass "ISR=2 storm had $($secondArmed.Acked) acknowledged and $($secondArmed.Running) in-flight calls at the kill point"
    Stop-CombinedNode $secondLeader -Force
    Assert-True $script:Nodes[$secondLeader].Process.HasExited "leader $secondLeader was forcibly terminated while follower $stalledFollower remained stalled"

    $dualFailureLive = @(1..5 | Where-Object { $_ -ne $stalledFollower -and $_ -ne $secondLeader })
    $dualControllerLeader = Wait-ForSharedLeader $dualFailureLive
    Pass "three surviving controllers retained quorum after the follower-plus-leader failure (leader $dualControllerLeader)"
    $secondFailoverImage = Wait-ForMetadataPredicate $dualFailureLive "sole surviving replica to become leader" {
        param($image)
        $partition = Get-TopicPartition $image
        return $null -ne $partition -and [int]$partition.leader -eq $survivingFollower -and
            @($partition.isr).Count -eq 1 -and [int]$partition.isr[0] -eq $survivingFollower
    }
    Pass "controller elected the sole remaining in-sync replica $survivingFollower"
    $secondAcknowledged = Complete-ProduceStorm $secondStorm
    $allAcknowledged = @($firstAcknowledged) + @($secondAcknowledged)
    $secondLatest = Assert-AcknowledgedContinuity $survivingFollower $allAcknowledged "leader failover during follower stall"
    Pass "ISR=2 failover lost none of the acknowledged prefix through offset $secondLatest"

    # The stopped old leader may naturally contain uncommitted bytes beyond
    # the new leader. Append one more CRC-valid old-epoch batch to make the
    # divergent crash image deterministic. This is done only while the old
    # leader is stopped; on restart KIP-101 reconciliation must remove it.
    $injection = Inject-DivergentTail $secondLeader
    $newLeaderState = Get-LogState $survivingFollower
    Assert-True ($injection.BaseOffset -ge $newLeaderState.LogEnd) "old leader crash image has a divergent tail beyond the new leader epoch end"
    Pass "recorded a CRC-valid divergent old-leader batch at offset $($injection.BaseOffset) for truncation proof"

    $null = Start-CombinedNode $stalledFollower -Restart
    Wait-ForHttpReady @($stalledFollower)
    $isrRecoveredTwo = Wait-ForMetadataPredicate (@($dualFailureLive) + @($stalledFollower)) "stalled follower catch-up and ISR re-entry" {
        param($image)
        $partition = Get-TopicPartition $image
        $broker = Get-MapValue $image.brokers $stalledFollower
        return $null -ne $partition -and $null -ne $broker -and [bool]$broker.alive -and
            @($partition.isr).Count -eq 2 -and (@($partition.isr) -contains $stalledFollower) -and
            (@($partition.isr) -contains $survivingFollower)
    }
    Pass "stalled follower caught up before re-entering ISR"

    $authoritativeStart = (Get-Offsets $survivingFollower).Latest
    [Int64]$authoritativeCount = $injection.EndOffset - $authoritativeStart + 8
    if ($authoritativeCount -lt 8) { $authoritativeCount = 8 }
    $authoritativeOutput = Invoke-Cli @(
        "--broker", (Get-BrokerAddress $survivingFollower),
        "produce", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--count", [string]$authoritativeCount,
        "--value-size", "128",
        "--acks", "all", "--timeout-ms", "30000"
    )
    Assert-True ($authoritativeOutput.Contains("produced $authoritativeCount records")) "new leader committed an authoritative replacement across the divergent offset"

    $null = Start-CombinedNode $secondLeader -Restart
    Wait-ForHttpReady @($secondLeader)
    $fullAfterSecond = Wait-ForFullIsr @(1, 2, 3, 4, 5) $replicas "second killed leader to catch up and restore full ISR"
    $reconciledBatch = Get-BatchAtOffset $secondLeader $injection.BaseOffset
    $leaderBatch = Get-BatchAtOffset $survivingFollower $injection.BaseOffset
    Assert-True ($null -ne $reconciledBatch -and $null -ne $leaderBatch) "rejoined old leader and new leader both contain the former divergence offset"
    $reconciledHash = Get-BytesSha256 $reconciledBatch
    $leaderHash = Get-BytesSha256 $leaderBatch
    Assert-True ($reconciledHash -eq $leaderHash -and $reconciledHash -ne $injection.Hash) "leader-epoch reconciliation truncated and replaced the divergent old-leader batch"
    Pass "all three replicas returned after the follower-stall failover"

    Write-Stage "Extended follower outage with continued acks=all production"
    $extendedPartition = Get-TopicPartition $fullAfterSecond
    $extendedLeader = [int]$extendedPartition.leader
    $extendedDown = @($replicas | Where-Object { $_ -ne $extendedLeader })[0]
    Stop-CombinedNode $extendedDown -Force
    $liveDuringExtended = @(1..5 | Where-Object { $_ -ne $extendedDown })
    $null = Wait-ForMetadataPredicate $liveDuringExtended "extended-down follower to leave ISR" {
        param($image)
        $partition = Get-TopicPartition $image
        $broker = Get-MapValue $image.brokers $extendedDown
        return $null -ne $partition -and $null -ne $broker -and -not [bool]$broker.alive -and
            @($partition.isr).Count -eq 2 -and -not (@($partition.isr) -contains $extendedDown)
    }
    $downState = Get-LogState $extendedDown
    Pass "follower $extendedDown stopped at persisted HWM $($downState.HighWatermark)"

    $backlogFile = Join-Path $script:WorkDir "extended-incompressible-records.txt"
    New-IncompressibleRecordFile $backlogFile $ExtendedBacklogRecords $ExtendedValueBytes
    $expectedBacklogBytes = [Int64]$ExtendedBacklogRecords * [Int64]$ExtendedValueBytes
    Assert-True ((Get-Item -LiteralPath $backlogFile).Length -ge $expectedBacklogBytes) "generated an incompressible on-disk backlog of at least $expectedBacklogBytes bytes"
    $bulkOutput = Invoke-Cli @(
        "--broker", (Get-BrokerAddress ([int]$witnesses[0])),
        "produce", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--file", $backlogFile,
        "--acks", "all",
        "--timeout-ms", "120000"
    )
    Assert-True ($bulkOutput.Contains("produced $ExtendedBacklogRecords records")) "acks=all built a substantial catch-up backlog while one follower was down"
    $outageStarted = [DateTime]::UtcNow
    $outageDeadline = $outageStarted.AddSeconds($ExtendedDowntimeSeconds)
    $continuedAcks = @()
    $nextProgress = $outageStarted.AddSeconds(60)
    $continuedIndex = 0
    while ([DateTime]::UtcNow -lt $outageDeadline) {
        $value = "extended-$continuedIndex-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
        $output = Invoke-AcksAllProduce ([int]$witnesses[1]) $value 30000
        $match = [Regex]::Match($output, '^acked offset=([0-9]+)$')
        if (-not $match.Success) {
            throw "cannot parse extended-outage ack: $output"
        }
        $continuedAcks += ,[PSCustomObject]@{ Offset = [Int64]$match.Groups[1].Value; Value = $value }
        $continuedIndex++
        if ([DateTime]::UtcNow -ge $nextProgress) {
            $elapsed = [int]([DateTime]::UtcNow - $outageStarted).TotalSeconds
            Write-Host "extended outage progress: ${elapsed}s, continued acks=$continuedIndex"
            $nextProgress = $nextProgress.AddSeconds(60)
        }
        if ([DateTime]::UtcNow -lt $outageDeadline) {
            Start-Sleep -Milliseconds $ExtendedProduceIntervalMs
        }
    }
    Assert-True ($continuedAcks.Count -gt 0) "production continued throughout the configured $ExtendedDowntimeSeconds-second outage"
    $liveOffsets = Get-Offsets ([int]$witnesses[0])
    [Int64]$initialLag = $liveOffsets.Latest - $downState.HighWatermark
    Assert-True ($initialLag -gt 0) "persisted offset lag grew to $initialLag while follower $extendedDown was down"

    $null = Start-CombinedNode $extendedDown -Restart
    Wait-ForHttpReady @($extendedDown)
    [Int64]$targetHwm = $liveOffsets.Latest
    $progressState = Wait-ForValue "restarted follower HWM to advance through an intermediate lag" {
        $state = Get-LogState $extendedDown
        if ($state.HighWatermark -gt $downState.HighWatermark -and $state.HighWatermark -lt $targetHwm) {
            return $state
        }
        return $null
    } $script:TimeoutSeconds 20
    [Int64]$intermediateLag = $targetHwm - $progressState.HighWatermark
    Assert-True ($intermediateLag -gt 0 -and $intermediateLag -lt $initialLag) "persisted catch-up lag visibly decreased from $initialLag to $intermediateLag"
    $fullAfterExtended = Wait-ForFullIsr @(1, 2, 3, 4, 5) $replicas "extended-down follower catch-up and ISR re-entry"
    $finalExtendedState = Get-LogState $extendedDown
    Assert-True ($finalExtendedState.HighWatermark -ge $targetHwm) "rejoined follower persisted the leader HWM after catch-up"
    $null = Assert-ReplicaPrefixIdentity $replicas "post-catch-up committed log prefix"

    Write-Stage "min.insync.replicas=2 rejection with two of three assigned replicas down"
    $minPartition = Get-TopicPartition $fullAfterExtended
    $minLeader = [int]$minPartition.leader
    $minFollowers = @($replicas | Where-Object { $_ -ne $minLeader })
    $beforeReject = Get-Offsets $minLeader
    $beforeRejectLog = Get-LogState $minLeader
    Stop-CombinedNode ([int]$minFollowers[0]) -Force
    $liveAfterOneMinKill = @(1..5 | Where-Object { $_ -ne [int]$minFollowers[0] })
    $null = Wait-ForMetadataPredicate $liveAfterOneMinKill "first min-ISR follower to leave ISR" {
        param($image)
        $partition = Get-TopicPartition $image
        return $null -ne $partition -and @($partition.isr).Count -eq 2 -and -not (@($partition.isr) -contains [int]$minFollowers[0])
    }
    Stop-CombinedNode ([int]$minFollowers[1]) -Force
    $minSurvivors = @(1..5 | Where-Object { $minFollowers -notcontains $_ })
    $minControllerLeader = Wait-ForSharedLeader $minSurvivors
    Pass "controller quorum remained live on nodes $($minSurvivors -join ',') after two assigned replicas were killed"
    $isrOneImage = Wait-ForMetadataPredicate $minSurvivors "two dead replicas and sole-leader ISR" {
        param($image)
        $partition = Get-TopicPartition $image
        return $null -ne $partition -and [int]$partition.leader -eq $minLeader -and
            @($partition.isr).Count -eq 1 -and [int]$partition.isr[0] -eq $minLeader -and
            (Test-BrokersAlive $image $minFollowers $false)
    }
    Pass "controller committed an ISR of one while two of three replicas were actually down"

    $proofName = "quorum-proof-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
    $proofOutput = Invoke-Cli @(
        "--broker", "invalid-broker-for-admin",
        "--controller", "http://127.0.0.1:$($script:Nodes[$minSurvivors[0]].ControlPort)",
        "topic", "create", "--name", $proofName,
        "--partitions", "1", "--replication-factor", "1"
    )
    Assert-True ($proofOutput.Contains("topic created name=`"$proofName`"")) "surviving three-controller quorum committed a metadata write"

    $rejected = Invoke-CliRaw @(
        "--broker", (Get-BrokerAddress $minLeader),
        "produce", "--topic", $script:Topic,
        "--partition", [string]$script:Partition,
        "--value", "must-not-append-below-min-isr",
        "--acks", "all", "--timeout-ms", "2000"
    )
    Assert-True ($rejected.ExitCode -ne 0) "acks=all produce failed while ISR was below min.insync.replicas"
    Assert-True ($rejected.Text -match 'server error 10: not enough in-sync replicas') "produce returned the stable NotEnoughReplicas error"
    $afterReject = Get-Offsets $minLeader
    $afterRejectLog = Get-LogState $minLeader
    Assert-True ($afterReject.Latest -eq $beforeReject.Latest) "rejected below-min-ISR produce did not advance the committed offset"
    Assert-True ($afterRejectLog.LogEnd -eq $beforeRejectLog.LogEnd) "rejected below-min-ISR produce did not append an uncommitted disk batch"

    $returningFollower = [int]$minFollowers[0]
    $null = Start-CombinedNode $returningFollower -Restart
    Wait-ForHttpReady @($returningFollower)
    $isrRestoredTwo = Wait-ForMetadataPredicate (@($minSurvivors) + @($returningFollower)) "one follower to catch up and restore min ISR" {
        param($image)
        $partition = Get-TopicPartition $image
        return $null -ne $partition -and @($partition.isr).Count -eq 2 -and
            (@($partition.isr) -contains $minLeader) -and (@($partition.isr) -contains $returningFollower)
    }
    Pass "one returning follower caught up and restored ISR to min.insync.replicas"
    $recoveryValue = "min-isr-recovered-" + [Guid]::NewGuid().ToString("N").Substring(0, 10)
    $recoveryOutput = Invoke-AcksAllProduce $minSurvivors[0] $recoveryValue 30000
    $recoveryMatch = [Regex]::Match($recoveryOutput, '^acked offset=([0-9]+)$')
    Assert-True $recoveryMatch.Success "acks=all production resumed after one follower returned"
    [Int64]$recoveryOffset = $recoveryMatch.Groups[1].Value
    Assert-True ($recoveryOffset -eq $beforeReject.Latest) "recovery append reused the offset that the rejected request did not consume"
    $recoveryRecord = Get-OneRecord $returningFollower $recoveryOffset
    Assert-True ($recoveryRecord.Value -eq $recoveryValue) "recovery record is readable through the returning follower seed"

    $lastFollower = [int]$minFollowers[1]
    $null = Start-CombinedNode $lastFollower -Restart
    Wait-ForHttpReady @($lastFollower)
    $null = Wait-ForFullIsr @(1, 2, 3, 4, 5) $replicas "final follower catch-up and full ISR restoration"
    $finalDigest = Assert-ReplicaPrefixIdentity $replicas "final committed log prefix"

    Write-Stage "M3 live verification complete"
    Write-Host ("replicas: [{0}], controller witnesses: [{1}]" -f ($replicas -join ', '), ($witnesses -join ', '))
    Write-Host ("final HWM={0} sha256={1}" -f $finalDigest.HighWatermark, $finalDigest.Hash)
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
