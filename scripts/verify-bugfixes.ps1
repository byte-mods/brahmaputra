param(
    [int]$SessionTimeoutMs = 3000,
    [switch]$KeepArtifacts
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$server = Join-Path $root "target\debug\brahmaputra-server.exe"
$cli = Join-Path $root "target\debug\brahmaputra-cli.exe"
if (-not (Test-Path -LiteralPath $server) -or -not (Test-Path -LiteralPath $cli)) {
    throw "build target/debug/brahmaputra-server.exe and brahmaputra-cli.exe first"
}

$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')
$script:RunDir = Join-Path $tempRoot ("brahmaputra-bugfix-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $script:RunDir | Out-Null
$clusterId = "bugfix-" + [Guid]::NewGuid().ToString("N")
$nodes = @{}
$dataPorts = @{}
$controlPorts = @{}

function Get-FreePort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
}

function Wait-Until([string]$Description, [int]$Seconds, [scriptblock]$Probe) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try { if (& $Probe) { return } } catch { }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "timed out waiting for $Description"
}

function Get-Json([int]$Node, [string]$Path) {
    Invoke-RestMethod -TimeoutSec 3 -Uri "http://127.0.0.1:$($controlPorts[$Node])$Path"
}

function Start-Node([int]$Node) {
    $nodeDir = Join-Path $script:RunDir "node-$Node"
    $dataDir = Join-Path $nodeDir "data"
    New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
    $arguments = [Collections.Generic.List[string]]::new()
    foreach ($argument in @(
        "--host", "127.0.0.1", "--port", [string]$dataPorts[$Node],
        "--data-dir", $dataDir, "--node-id", [string]$Node,
        "--cluster-id", $clusterId, "--control-port", [string]$controlPorts[$Node],
        "--heartbeat-interval-ms", "500", "--session-timeout-ms", [string]$SessionTimeoutMs,
        "--http-port", "0", "--offsets-topic-partitions", "1"
    )) { $arguments.Add($argument) }
    foreach ($peer in 1..3) {
        $arguments.Add("--controller-peer")
        $arguments.Add("$peer=127.0.0.1:$($controlPorts[$peer])")
    }
    $process = Start-Process -FilePath $server -ArgumentList $arguments.ToArray() `
        -RedirectStandardOutput (Join-Path $nodeDir "stdout.log") `
        -RedirectStandardError (Join-Path $nodeDir "stderr.log") `
        -WindowStyle Hidden -PassThru
    $nodes[$Node] = $process
}

function Invoke-Cli([string[]]$Arguments) {
    $output = & $cli @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "CLI failed ($LASTEXITCODE): $($Arguments -join ' ')`n$($output -join "`n")"
    }
    return ($output -join "`n")
}

function Topic-Partition([object]$Metadata, [string]$Topic) {
    return $Metadata.topics.$Topic.partitions.'0'
}

function Marker-Path([int]$Node, [string]$Topic) {
    return (Join-Path $script:RunDir "node-$Node\data\$Topic-0\.topic-epoch")
}

function Marker-Hex([int]$Node, [string]$Topic) {
    $path = Marker-Path $Node $Topic
    if (-not (Test-Path -LiteralPath $path)) { return $null }
    return (([IO.File]::ReadAllBytes($path) | ForEach-Object { $_.ToString("X2") }) -join "")
}

try {
    foreach ($node in 1..3) {
        $dataPorts[$node] = Get-FreePort
        $controlPorts[$node] = Get-FreePort
    }
    foreach ($node in 1..3) { Start-Node $node }
    Wait-Until "three controller HTTP listeners" 30 {
        foreach ($node in 1..3) { if (-not (Get-Json $node "/api/v1/controller/raft")) { return $false } }
        return $true
    }
    Invoke-RestMethod -Method Post -TimeoutSec 20 -Uri "http://127.0.0.1:$($controlPorts[1])/api/v1/controller/bootstrap" | Out-Null
    Wait-Until "all brokers to register" 45 {
        $metadata = Get-Json 1 "/api/v1/controller/metadata"
        return @($metadata.brokers.psobject.Properties.Value | Where-Object alive).Count -eq 3
    }
    Write-Host "PASS: three brokers registered"

    $controller = "http://127.0.0.1:$($controlPorts[1])"
    $broker = "127.0.0.1:$($dataPorts[1])"
    Invoke-Cli @("--controller", $controller, "topic", "create", "--name", "reused", "--partitions", "1", "--replication-factor", "3") | Out-Null
    Wait-Until "reused topic full ISR" 45 {
        $partition = Topic-Partition (Get-Json 1 "/api/v1/controller/metadata") "reused"
        return $null -ne $partition -and @($partition.isr).Count -eq 3
    }
    Invoke-Cli @("--broker", $broker, "produce", "--topic", "reused", "--partition", "0", "--value", "deleted-value", "--acks", "all") | Out-Null
    Wait-Until "incarnation markers on every replica" 30 {
        return ((Test-Path -LiteralPath (Marker-Path 1 "reused")) -and
            (Test-Path -LiteralPath (Marker-Path 2 "reused")) -and
            (Test-Path -LiteralPath (Marker-Path 3 "reused")))
    }
    $oldMarkers = @{}
    foreach ($node in 1..3) { $oldMarkers[$node] = Marker-Hex $node "reused" }

    # Deliberately recreate immediately: brokers may coalesce the metadata
    # images and never observe the intermediate absence.
    Invoke-Cli @("--controller", $controller, "topic", "delete", "--name", "reused") | Out-Null
    Invoke-Cli @("--controller", $controller, "topic", "create", "--name", "reused", "--partitions", "1", "--replication-factor", "3") | Out-Null
    Wait-Until "fresh recreated partition" 45 {
        try {
            $read = Invoke-Cli @("--broker", $broker, "consume", "--topic", "reused", "--partition", "0", "--from", "earliest", "--max", "10")
            return ($read -notmatch "deleted-value")
        } catch { return $false }
    }
    Invoke-Cli @("--broker", $broker, "produce", "--topic", "reused", "--partition", "0", "--value", "fresh-value", "--acks", "all") | Out-Null
    $read = Invoke-Cli @("--broker", $broker, "consume", "--topic", "reused", "--partition", "0", "--from", "earliest", "--max", "10")
    if ($read -match "deleted-value" -or $read -notmatch "fresh-value") {
        throw "recreated topic returned wrong data:`n$read"
    }
    foreach ($node in 1..3) {
        if ((Marker-Hex $node "reused") -eq $oldMarkers[$node]) {
            throw "node $node retained the deleted topic incarnation marker"
        }
    }
    Write-Host "PASS: immediate delete/recreate discarded every old replica and exposed only fresh-value"

    $raft = Get-Json 1 "/api/v1/controller/raft"
    $dead = [int]$raft.current_leader
    if ($dead -lt 1 -or $dead -gt 3) { throw "invalid Raft leader: $dead" }
    Stop-Process -Id $nodes[$dead].Id -Force
    $nodes[$dead].WaitForExit(5000) | Out-Null
    Write-Host "INFO: hard-killed controller leader node $dead"
    $survivors = @(1..3 | Where-Object { $_ -ne $dead })

    # This exceeds the configured lease by a wide margin. Before the fix,
    # both survivors irreversibly fenced themselves during this window.
    Start-Sleep -Milliseconds ($SessionTimeoutMs * 3)
    foreach ($node in $survivors) {
        if ($nodes[$node].HasExited) { throw "surviving node $node self-terminated after controller outage" }
    }
    Wait-Until "surviving controllers to elect one leader" 30 {
        $leaders = @()
        foreach ($node in $survivors) { $leaders += [string](Get-Json $node "/api/v1/controller/raft").current_leader }
        return $leaders[0] -ne "" -and $leaders[0] -eq $leaders[1] -and $survivors -contains [int]$leaders[0]
    }
    $observer = $survivors[0]
    $controller = "http://127.0.0.1:$($controlPorts[$observer])"
    $broker = "127.0.0.1:$($dataPorts[$observer])"
    Wait-Until "surviving broker leases to be active" 30 {
        try {
            Invoke-Cli @("--broker", $broker, "produce", "--topic", "reused", "--partition", "0", "--value", "after-controller-outage", "--acks", "all", "--timeout-ms", "5000") | Out-Null
            return $true
        } catch { return $false }
    }
    Write-Host "PASS: surviving quorum remained alive, renewed broker leases, accepted data, and committed metadata"

    # Remove the second controller as well. The remaining process must stop
    # serving after its lease, but it must not exit; restoring one peer must
    # let it conditionally re-register and resume.
    $keeper = $survivors[0]
    $secondDead = $survivors[1]
    Stop-Process -Id $nodes[$secondDead].Id -Force
    $nodes[$secondDead].WaitForExit(5000) | Out-Null
    Start-Sleep -Milliseconds ($SessionTimeoutMs * 3)
    if ($nodes[$keeper].HasExited) {
        throw "last broker self-terminated while its controller quorum was unavailable"
    }
    $servedWithoutLease = $false
    try {
        Invoke-Cli @(
            "--broker", "127.0.0.1:$($dataPorts[$keeper])", "produce",
            "--topic", "reused", "--partition", "0", "--value", "must-not-serve",
            "--acks", "1", "--timeout-ms", "1500"
        ) | Out-Null
        $servedWithoutLease = $true
    } catch { }
    if ($servedWithoutLease) { throw "broker served data after its controller lease expired" }
    Write-Host "PASS: quorum loss suspended the data plane without terminating the broker process"

    Start-Node $secondDead
    Wait-Until "restored controller peer" 30 { return $null -ne (Get-Json $secondDead "/api/v1/controller/raft") }
    Wait-Until "conditional lease recovery" 45 {
        try {
            Invoke-Cli @(
                "--broker", "127.0.0.1:$($dataPorts[$keeper])", "produce",
                "--topic", "reused", "--partition", "0", "--value", "after-lease-recovery",
                "--acks", "all", "--timeout-ms", "5000"
            ) | Out-Null
            return $true
        } catch { return $false }
    }
    $controller = "http://127.0.0.1:$($controlPorts[$keeper])"
    Invoke-Cli @("--controller", $controller, "topic", "create", "--name", "after-outage", "--partitions", "1", "--replication-factor", "1") | Out-Null
    Write-Host "PASS: restoring quorum re-registered the suspended process and resumed data and metadata writes"
    Write-Host "PASS: manual bug-fix verification complete"
}
finally {
    foreach ($process in $nodes.Values) {
        try { if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force } } catch { }
    }
    foreach ($process in $nodes.Values) { try { $process.WaitForExit(5000) | Out-Null } catch { } }

    if ($KeepArtifacts) {
        Write-Host "artifacts: $script:RunDir"
    } else {
        $resolved = [IO.Path]::GetFullPath($script:RunDir)
        $leaf = Split-Path -Leaf $resolved
        $parent = (Split-Path -Parent $resolved).TrimEnd('\')
        if ($parent -ne $tempRoot -or $leaf -notmatch '^brahmaputra-bugfix-[0-9a-f]{32}$') {
            throw "refusing to remove unexpected path $resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
