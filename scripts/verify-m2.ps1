# M2 live verification: real combined broker/controller processes, HTTP admin
# requests, data-plane Metadata RPCs, Raft failover, broker fencing, and rejoin.
#
# This script intentionally targets Windows PowerShell 5 as well as newer
# PowerShell releases. Run it from any directory:
#
#   powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\verify-m2.ps1

[CmdletBinding()]
param(
    [int]$TimeoutSeconds = 60,
    [int]$HeartbeatIntervalMs = 500,
    [int]$SessionTimeoutMs = 5000
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

$script:Checks = 0
$script:Succeeded = $false
$script:Nodes = @{}
$script:AllocatedPorts = New-Object 'System.Collections.Generic.HashSet[int]'
$script:ProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$script:WorkDir = Join-Path ([IO.Path]::GetTempPath()) ("brahmaputra-m2-" + [Guid]::NewGuid().ToString("N"))
$script:ClusterId = "m2-live-" + [Guid]::NewGuid().ToString("N").Substring(0, 12)
$script:ServerExe = $null
$script:CliExe = $null

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
        [int]$DelayMilliseconds = 150
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
    return Invoke-RestMethod -Uri $uri -Method Get -TimeoutSec 3
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
        return Invoke-RestMethod -Uri $uri -Method Post -TimeoutSec 12
    }
    $json = $Body | ConvertTo-Json -Depth 20 -Compress
    return Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json" -Body $json -TimeoutSec 12
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
        $errorProperty = Get-ResultProperty $Response "Err"
        $details = $Response | ConvertTo-Json -Depth 20 -Compress
        if ($null -ne $errorProperty) {
            $details = $errorProperty.Value | ConvertTo-Json -Depth 20 -Compress
        }
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
    $stdout = Join-Path $nodeDir "server.stdout.log"
    $stderr = Join-Path $nodeDir "server.stderr.log"
    if ($Restart) {
        $stdout = Join-Path $nodeDir "server.restart.stdout.log"
        $stderr = Join-Path $nodeDir "server.restart.stderr.log"
    }
    $null = New-Item -ItemType Directory -Force -Path $dataDir

    $arguments = @(
        "--host", "127.0.0.1",
        "--port", [string]$script:NodePorts[$NodeId].Data,
        "--data-dir", $dataDir,
        "--node-id", [string]$NodeId,
        "--cluster-id", $script:ClusterId,
        "--control-port", [string]$script:NodePorts[$NodeId].Control,
        "--heartbeat-interval-ms", [string]$HeartbeatIntervalMs,
        "--session-timeout-ms", [string]$SessionTimeoutMs
    )
    foreach ($peerId in 1..3) {
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
    Write-Host ("started node {0}: pid={1} data=127.0.0.1:{2} control=127.0.0.1:{3}" -f $NodeId, $process.Id, $descriptor.DataPort, $descriptor.ControlPort)
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
    $node = $script:Nodes[$NodeId]
    $process = $node.Process
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

function Wait-ForHttpReady {
    param([int[]]$NodeIds)
    foreach ($nodeId in $NodeIds) {
        $null = Wait-ForValue ("node $nodeId controller HTTP readiness") {
            $node = $script:Nodes[$nodeId]
            $node.Process.Refresh()
            if ($node.Process.HasExited) {
                throw "node $nodeId exited with code $($node.Process.ExitCode)"
            }
            $metrics = Invoke-ControllerGet $nodeId "/api/v1/controller/raft"
            if ($null -ne $metrics) { return $true }
            return $null
        } 15 100
    }
}

function Wait-ForSharedLeader {
    param([int[]]$NodeIds)
    return Wait-ForValue ("controllers $($NodeIds -join ',') to agree on a live Raft leader") {
        $leaders = @()
        foreach ($nodeId in $NodeIds) {
            $metrics = Invoke-ControllerGet $nodeId "/api/v1/controller/raft"
            if ($null -eq $metrics.current_leader) {
                return $null
            }
            $leaders += [int]$metrics.current_leader
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

function Invoke-Cli {
    param([string[]]$Arguments)
    $lines = @(& $script:CliExe @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $text = (($lines | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    if ($exitCode -ne 0) {
        throw "CLI exited with code $exitCode while running '$($Arguments -join ' ')':`n$text"
    }
    return $text
}

function Invoke-TopicCreateCli {
    param(
        [int]$ControllerNodeId,
        [string]$Name,
        [int]$Partitions,
        [int]$ReplicationFactor
    )
    $controller = "http://127.0.0.1:$($script:Nodes[$ControllerNodeId].ControlPort)"
    return Invoke-Cli @(
        "--broker", "deliberately-invalid-broker-address",
        "--controller", $controller,
        "topic", "create",
        "--name", $Name,
        "--partitions", [string]$Partitions,
        "--replication-factor", [string]$ReplicationFactor
    )
}

function Get-TcpMetadata {
    param(
        [int]$NodeId,
        [string]$Topic
    )
    $broker = "127.0.0.1:$($script:Nodes[$NodeId].DataPort)"
    return Invoke-Cli @("--broker", $broker, "metadata", "--topic", $Topic)
}

function Format-IdVector {
    param([object[]]$Values)
    $ids = @($Values | ForEach-Object { [string][int]$_ })
    return "[" + ($ids -join ", ") + "]"
}

function Test-TcpMetadataAgainstImage {
    param(
        [string]$Output,
        [object]$Image,
        [string]$TopicName
    )
    if ($Output -notmatch ("(?m)^controller: " + [Regex]::Escape([string]$Image.controller_id) + "\s*$")) {
        return $false
    }
    foreach ($brokerEntry in (Get-MapEntries $Image.brokers)) {
        $broker = $brokerEntry.Value
        $line = "broker $($broker.broker_id): $($broker.host):$($broker.data_port)"
        if (-not $Output.Contains($line)) {
            return $false
        }
    }
    $topic = Get-MapValue $Image.topics $TopicName
    if ($null -eq $topic -or -not $Output.Contains("topic `"$TopicName`" (error_code=0):")) {
        return $false
    }
    $partitions = @(Get-MapEntries $topic.partitions | Sort-Object { [int]$_.Name })
    foreach ($partitionEntry in $partitions) {
        $partition = $partitionEntry.Value
        $replicas = Format-IdVector @($partition.replicas)
        $isr = Format-IdVector @($partition.isr)
        $line = "partition $($partition.partition): leader=$($partition.leader) replicas=$replicas isr=$isr leader_epoch=$($partition.leader_epoch)"
        if (-not $Output.Contains($line)) {
            return $false
        }
    }
    return $true
}

function Wait-ForMatchingTcpMetadata {
    param(
        [int[]]$NodeIds,
        [object]$Image,
        [string]$TopicName,
        [string]$Description
    )
    return Wait-ForValue $Description {
        $outputs = @()
        foreach ($nodeId in $NodeIds) {
            $output = Get-TcpMetadata $nodeId $TopicName
            if (-not (Test-TcpMetadataAgainstImage $output $Image $TopicName)) {
                return $null
            }
            $outputs += $output
        }
        $unique = @($outputs | Select-Object -Unique)
        if ($unique.Count -eq 1) {
            return $unique[0]
        }
        return $null
    }
}

function Test-AllBrokersAlive {
    param(
        [object]$Image,
        [int[]]$BrokerIds
    )
    foreach ($brokerId in $BrokerIds) {
        $broker = Get-MapValue $Image.brokers $brokerId
        if ($null -eq $broker -or -not [bool]$broker.alive) {
            return $false
        }
    }
    return $true
}

function Test-TopicFullyReplicated {
    param(
        [object]$Image,
        [string]$TopicName,
        [int]$PartitionCount,
        [int]$ReplicaCount
    )
    $topic = Get-MapValue $Image.topics $TopicName
    if ($null -eq $topic) {
        return $false
    }
    $partitions = @(Get-MapEntries $topic.partitions)
    if ($partitions.Count -ne $PartitionCount) {
        return $false
    }
    foreach ($entry in $partitions) {
        if (@($entry.Value.replicas).Count -ne $ReplicaCount -or @($entry.Value.isr).Count -ne $ReplicaCount) {
            return $false
        }
    }
    return $true
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
            if ($node.Process.HasExited) {
                $status = "exited code=$($node.Process.ExitCode)"
            }
            else {
                $status = "running"
            }
        }
        catch {
            $status = "process unavailable"
        }
        Write-Host ("node {0}: pid={1} status={2}" -f $nodeId, $node.Process.Id, $status)
        Write-Host ("  args: " + $node.Arguments)
        foreach ($logPath in @($node.Stdout, $node.Stderr)) {
            Write-Host ("  tail " + $logPath)
            if (Test-Path -LiteralPath $logPath) {
                Get-Content -LiteralPath $logPath -Tail 80 -ErrorAction SilentlyContinue | ForEach-Object {
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
    if (-not $leaf.StartsWith("brahmaputra-m2-", [StringComparison]::OrdinalIgnoreCase)) {
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

    $null = New-Item -ItemType Directory -Force -Path $script:WorkDir
    Set-Location -LiteralPath $script:ProjectRoot

    Write-Stage "Build the actual server and CLI binaries"
    & cargo build -p brahmaputra-server -p brahmaputra-cli
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }
    $exeSuffix = ""
    if ($env:OS -eq "Windows_NT") {
        $exeSuffix = ".exe"
    }
    $script:ServerExe = Join-Path $script:ProjectRoot ("target\debug\brahmaputra-server" + $exeSuffix)
    $script:CliExe = Join-Path $script:ProjectRoot ("target\debug\brahmaputra-cli" + $exeSuffix)
    Assert-True (Test-Path -LiteralPath $script:ServerExe) "server binary exists"
    Assert-True (Test-Path -LiteralPath $script:CliExe) "CLI binary exists"

    $script:NodePorts = @{}
    foreach ($nodeId in 1..3) {
        $script:NodePorts[$nodeId] = [PSCustomObject]@{
            Data = Get-FreeTcpPort
            Control = Get-FreeTcpPort
        }
    }

    Write-Stage "Launch a fixed three-member combined controller/broker cluster"
    foreach ($nodeId in 1..3) {
        $null = Start-CombinedNode $nodeId
    }
    Wait-ForHttpReady @(1, 2, 3)
    Pass "all three controller HTTP endpoints became ready"

    $bootstrapResult = Invoke-ControllerPost 1 "/api/v1/controller/bootstrap"
    $null = Assert-ControllerResultOk $bootstrapResult "controller quorum bootstrapped through HTTP"
    $firstLeader = Wait-ForSharedLeader @(1, 2, 3)
    Pass "all controllers agreed on initial leader $firstLeader"

    $initialImage = Wait-ForMetadataPredicate @(1, 2, 3) "three live broker registrations and active-controller metadata" {
        param($image)
        return (Test-AllBrokersAlive $image @(1, 2, 3)) -and [int]$image.controller_id -eq $firstLeader
    }
    Assert-True ((Get-MapEntries $initialImage.brokers).Count -eq 3) "all three combined nodes registered as brokers"

    $initialEpochs = @{}
    foreach ($brokerId in 1..3) {
        $initialEpochs[$brokerId] = [UInt64](Get-MapValue $initialImage.brokers $brokerId).broker_epoch
    }

    $firstFollower = @(1..3 | Where-Object { $_ -ne $firstLeader })[0]
    Write-Stage "Create a replicated topic through a nonleader controller"
    $createOutput = Invoke-TopicCreateCli $firstFollower "orders" 6 3
    Assert-True ($createOutput.Contains('topic created name="orders" partitions=6 replication_factor=3')) "CLI created orders through follower $firstFollower"

    $topicImage = Wait-ForMetadataPredicate @(1, 2, 3) "orders metadata to converge on all controllers" {
        param($image)
        return [int]$image.controller_id -eq $firstLeader -and (Test-TopicFullyReplicated $image "orders" 6 3)
    }
    Pass "orders has six partitions with replication factor three and full ISR"
    $tcpImage = Wait-ForMatchingTcpMetadata @(1, 2, 3) $topicImage "orders" "identical TCP Metadata images from all three brokers"
    Assert-True ($tcpImage.Contains('leader_epoch=0')) "TCP Metadata exposes partition leader epochs"
    Pass "all live brokers returned identical leaders, replicas, ISR, and epochs"

    $ordersBeforeKill = Get-MapValue $topicImage.topics "orders"
    $affectedBeforeKill = @(
        Get-MapEntries $ordersBeforeKill.partitions |
            Where-Object { [int]$_.Value.leader -eq $firstLeader } |
            Sort-Object { [int]$_.Name }
    )
    Assert-True ($affectedBeforeKill.Count -gt 0) "initial controller/broker leader owns at least one partition leader"
    $affectedPartitionId = [int]$affectedBeforeKill[0].Name
    $affectedEpochBefore = [int]$affectedBeforeKill[0].Value.leader_epoch

    Write-Stage "Hard-kill the active combined node and observe quorum plus broker failover"
    $killedNodeId = $firstLeader
    Stop-CombinedNode $killedNodeId -Force
    $script:Nodes[$killedNodeId].Process.Refresh()
    Assert-True $script:Nodes[$killedNodeId].Process.HasExited "active combined node $killedNodeId was forcibly terminated"

    $survivors = @(1..3 | Where-Object { $_ -ne $killedNodeId })
    $secondLeader = Wait-ForSharedLeader $survivors
    Assert-True ($secondLeader -ne $firstLeader) "surviving quorum elected a different controller leader ($secondLeader)"

    $fencedImage = Wait-ForMetadataPredicate $survivors "broker lease fencing and ISR elections after the hard kill" {
        param($image)
        $dead = Get-MapValue $image.brokers $killedNodeId
        if ($null -eq $dead -or [bool]$dead.alive -or [int]$image.controller_id -ne $secondLeader) {
            return $false
        }
        if (-not (Test-AllBrokersAlive $image $survivors)) {
            return $false
        }
        $topic = Get-MapValue $image.topics "orders"
        if ($null -eq $topic) {
            return $false
        }
        foreach ($entry in (Get-MapEntries $topic.partitions)) {
            $partition = $entry.Value
            if (@($partition.isr) -contains $killedNodeId) {
                return $false
            }
            if ([int]$partition.leader -eq $killedNodeId -or -not (@($partition.isr) -contains [int]$partition.leader)) {
                return $false
            }
        }
        $affected = Get-MapValue $topic.partitions $affectedPartitionId
        return $null -ne $affected -and [int]$affected.leader_epoch -gt $affectedEpochBefore
    }
    Pass "expired broker $killedNodeId was fenced, removed from ISR, and its partition leaders were re-elected"
    $survivorTcp = Wait-ForMatchingTcpMetadata $survivors $fencedImage "orders" "identical post-failover TCP Metadata from both surviving brokers"
    $fencedOrders = Get-MapValue $fencedImage.topics "orders"
    $fencedAffected = Get-MapValue $fencedOrders.partitions $affectedPartitionId
    $fencedEpoch = [int]$fencedAffected.leader_epoch
    Assert-True ($fencedEpoch -gt $affectedEpochBefore -and $survivorTcp.Contains("leader_epoch=$fencedEpoch")) "post-failover TCP Metadata exposes the bumped leader epoch $fencedEpoch"

    Write-Stage "Write through the surviving nonleader after controller failover"
    $secondFollower = @($survivors | Where-Object { $_ -ne $secondLeader })[0]
    $secondCreateOutput = Invoke-TopicCreateCli $secondFollower "after-failover" 4 2
    Assert-True ($secondCreateOutput.Contains('topic created name="after-failover" partitions=4 replication_factor=2')) "CLI created a second topic through follower $secondFollower"
    $afterFailoverImage = Wait-ForMetadataPredicate $survivors "second topic metadata after failover" {
        param($image)
        return (Test-TopicFullyReplicated $image "after-failover" 4 2)
    }
    Pass "post-failover topic has four partitions with the two surviving replicas"

    Write-Stage "Restart the killed node and verify fencing epoch plus ISR rejoin"
    $null = Start-CombinedNode $killedNodeId -Restart
    Wait-ForHttpReady @($killedNodeId)
    Pass "restarted controller/broker endpoint became ready"
    $thirdLeader = Wait-ForSharedLeader @(1, 2, 3)
    Pass "all three controllers converged after restart with leader $thirdLeader"

    $rejoinedImage = Wait-ForMetadataPredicate @(1, 2, 3) "restarted broker registration and ISR rejoin" {
        param($image)
        $broker = Get-MapValue $image.brokers $killedNodeId
        if ($null -eq $broker -or -not [bool]$broker.alive -or [UInt64]$broker.broker_epoch -le $initialEpochs[$killedNodeId]) {
            return $false
        }
        $topic = Get-MapValue $image.topics "orders"
        if ($null -eq $topic) {
            return $false
        }
        foreach ($entry in (Get-MapEntries $topic.partitions)) {
            if (-not (@($entry.Value.isr) -contains $killedNodeId)) {
                return $false
            }
        }
        return $true
    }
    $rejoinedBroker = Get-MapValue $rejoinedImage.brokers $killedNodeId
    Assert-True ([UInt64]$rejoinedBroker.broker_epoch -gt $initialEpochs[$killedNodeId]) "broker $killedNodeId epoch increased from $($initialEpochs[$killedNodeId]) to $($rejoinedBroker.broker_epoch)"
    Pass "restarted broker rejoined every assigned orders ISR"
    $rejoinedTcp = Wait-ForMatchingTcpMetadata @(1, 2, 3) $rejoinedImage "orders" "identical TCP Metadata after broker/controller restart"
    Assert-True ($rejoinedTcp.Contains("broker $killedNodeId`: 127.0.0.1:$($script:Nodes[$killedNodeId].DataPort)")) "restarted broker is advertised through every TCP Metadata endpoint"

    Write-Stage "Route explicit-partition data-plane calls from follower seeds"
    $routingOrders = Get-MapValue $rejoinedImage.topics "orders"
    $routingPartitionEntry = @(Get-MapEntries $routingOrders.partitions | Sort-Object { [int]$_.Name })[0]
    $routingPartition = [int]$routingPartitionEntry.Name
    $routingLeader = [int]$routingPartitionEntry.Value.leader
    $routingSeeds = @(1..3 | Where-Object { $_ -ne $routingLeader })
    Assert-True ($routingSeeds.Count -eq 2) "selected partition $routingPartition has two distinct follower seeds"

    $produceSeed = [int]$routingSeeds[0]
    $readSeed = [int]$routingSeeds[1]
    $produceBroker = "127.0.0.1:$($script:Nodes[$produceSeed].DataPort)"
    $readBroker = "127.0.0.1:$($script:Nodes[$readSeed].DataPort)"
    $routedValue = "m2-routed-value"
    $produceOutput = Invoke-Cli @(
        "--broker", $produceBroker,
        "produce", "--topic", "orders",
        "--partition", [string]$routingPartition,
        "--value", $routedValue
    )
    $produceMatch = [Regex]::Match($produceOutput, '^acked offset=([0-9]+)$')
    Assert-True $produceMatch.Success "produce seeded at follower broker $produceSeed routed to partition leader $routingLeader"
    $producedOffset = [Int64]$produceMatch.Groups[1].Value

    $offsetOutput = Invoke-Cli @(
        "--broker", $readBroker,
        "offsets", "--topic", "orders",
        "--partition", [string]$routingPartition
    )
    $expectedLatest = $producedOffset + 1
    Assert-True ($offsetOutput.Contains("orders-$routingPartition`: earliest=0 latest=$expectedLatest")) "offset lookup seeded at follower broker $readSeed reached the partition leader"

    $consumeOutput = Invoke-Cli @(
        "--broker", $readBroker,
        "consume", "--topic", "orders",
        "--partition", [string]$routingPartition,
        "--from", "earliest", "--max", "1"
    )
    $expectedRecord = "partition=$routingPartition offset=$producedOffset key=- value=$routedValue"
    Assert-True ($consumeOutput.Contains($expectedRecord)) "consume seeded at follower broker $readSeed routed offset lookup and fetch to leader $routingLeader"

    Write-Stage "Inject a stale leader-epoch metadata mutation through HTTP"
    $currentOrders = Get-MapValue $rejoinedImage.topics "orders"
    $currentPartition = Get-MapValue $currentOrders.partitions $affectedPartitionId
    $currentEpoch = [int]$currentPartition.leader_epoch
    Assert-True ($currentEpoch -gt 0) "selected partition has a nonzero leader epoch"
    $staleEpoch = $currentEpoch - 1
    $staleCommand = [ordered]@{
        type = "change_partition"
        topic = "orders"
        partition = $affectedPartitionId
        leader = [int]$currentPartition.leader
        isr = @($currentPartition.isr | ForEach-Object { [int]$_ })
        expected_leader_epoch = $staleEpoch
    }
    $commandFollower = @(1..3 | Where-Object { $_ -ne $thirdLeader })[0]
    $staleResponse = Invoke-ControllerPost $commandFollower "/api/v1/controller/command" $staleCommand
    $errorProperty = Get-ResultProperty $staleResponse "Err"
    Assert-True ($null -ne $errorProperty) "stale ChangePartition command was rejected"
    Assert-True ([string]$errorProperty.Value.code -eq "metadata_rejected") "stale command returned the stable metadata_rejected code"
    Assert-True ([string]$errorProperty.Value.message -match "stale leader epoch") "stale command error names the leader-epoch violation"

    $unchangedImage = Wait-ForMetadataPredicate @(1, 2, 3) "rejected stale mutation to remain unapplied" {
        param($image)
        $topic = Get-MapValue $image.topics "orders"
        if ($null -eq $topic) { return $false }
        $partition = Get-MapValue $topic.partitions $affectedPartitionId
        return $null -ne $partition -and [int]$partition.leader_epoch -eq $currentEpoch -and [int]$partition.leader -eq [int]$currentPartition.leader
    }
    Assert-True ([int](Get-MapValue (Get-MapValue $unchangedImage.topics "orders").partitions $affectedPartitionId).leader_epoch -eq $currentEpoch) "rejected stale command did not mutate partition metadata"

    Write-Stage "M2 live verification complete"
    Write-Host ("leaders: initial={0}, after-kill={1}, after-restart={2}" -f $firstLeader, $secondLeader, $thirdLeader)
    Write-Host ("killed/rejoined broker: id={0}, epoch={1}->{2}" -f $killedNodeId, $initialEpochs[$killedNodeId], $rejoinedBroker.broker_epoch)
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
