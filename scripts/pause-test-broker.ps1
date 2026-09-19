param(
    [Parameter(Mandatory = $true)][int]$ProcessId,
    [Parameter(Mandatory = $true)][string]$ExpectedExecutable,
    [Parameter(Mandatory = $true)][ValidateSet('Suspend', 'Resume')][string]$Action
)
$ErrorActionPreference = 'Stop'
$brokerProcess = Get-Process -Id $ProcessId
$expectedPath = [IO.Path]::GetFullPath($ExpectedExecutable)
if ($brokerProcess.ProcessName -ne 'brahmaputra-server' -or
    $brokerProcess.Path -ne $expectedPath) {
    throw 'Refusing to pause a process other than the expected test broker'
}
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class TestBrokerPause {
    [DllImport("ntdll.dll")]
    public static extern int NtSuspendProcess(IntPtr handle);
    [DllImport("ntdll.dll")]
    public static extern int NtResumeProcess(IntPtr handle);
}
'@
if ($Action -eq 'Suspend') {
    $status = [TestBrokerPause]::NtSuspendProcess($brokerProcess.Handle)
} else {
    $status = [TestBrokerPause]::NtResumeProcess($brokerProcess.Handle)
}
if ($status -ne 0) { throw "$Action failed with NTSTATUS $status" }
