#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build, rewrite, and run the Coyote systematic-concurrency tests for Sluice's lock-free rings.

.DESCRIPTION
    Coyote takes control of the scheduler and explores the producer/consumer interleavings exhaustively
    (within a step bound), proving the ring never loses, reorders, duplicates, or tears a message under any
    of them. The assemblies must be *rewritten* before the scheduler can intercept them — this script does
    the full build -> rewrite -> test cycle.

    Requires the Coyote CLI:  dotnet tool install --global Microsoft.Coyote.CLI --version 1.7.11

.PARAMETER Iterations
    Number of distinct schedules to explore per test (default 1000).
#>
param(
    [int]$Iterations = 1000
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$proj = Join-Path $here '..' 'tests' 'Sluice.Concurrency'
Push-Location $proj
try {
    if (-not (Get-Command coyote -ErrorAction SilentlyContinue)) {
        throw "The 'coyote' CLI was not found on PATH. Install it with: dotnet tool install --global Microsoft.Coyote.CLI --version 1.7.11"
    }

    Write-Host '== build ==' -ForegroundColor Cyan
    dotnet build Sluice.Concurrency.csproj -c Release --nologo

    Write-Host '== rewrite ==' -ForegroundColor Cyan
    coyote rewrite rewrite.coyote.json

    $dll = 'bin/Release/net8.0/Sluice.Concurrency.dll'
    $tests = @(
        'Sluice.Concurrency.RingConcurrencyTests.Spsc_ring_never_loses_reorders_or_tears_a_message',
        'Sluice.Concurrency.RingConcurrencyTests.Read_in_place_slot_is_not_reclaimed_before_AdvanceRead'
    )

    $failed = $false
    foreach ($t in $tests) {
        Write-Host "== test: $t ==" -ForegroundColor Cyan
        coyote test $dll -m $t -i $Iterations
        if ($LASTEXITCODE -ne 0) { $failed = $true }
    }

    if ($failed) { Write-Host 'Coyote found a bug — see the replayable .schedule trace above.' -ForegroundColor Red; exit 1 }
    Write-Host 'All Coyote tests passed (0 bugs).' -ForegroundColor Green
}
finally {
    Pop-Location
}
