# RavenDB-24528 - Windows car-dealership crash matrix (parallel x2).
# Runs test/Tryouts `carscenario` per WriteMode: seed (dca/di/dd) -> ro+rq load -> hard-kill -> recover ->
# integrity(export/import) + index-error + recovery-log ERROR/FATAL asserts, looped per mode.
# Auto runs alone first, then FileIo|Mmap concurrently, then IoRing|IoRing-256 concurrently (2 slots, ports 8080-8082).
# Live dashboard: D:\temp\ravendb-24528\progress.html (auto-refresh). Windows-only (hardcoded D: paths);
# on Linux use carscenario directly with QA_CLIENT_DIR / --qa-dir (see 00-REFERENCE.md sections 7-8).
$ErrorActionPreference = 'Continue'
$repo = "D:\workspace\ravendb-8.0"
Set-Location $repo
dotnet build test/Tryouts -c Release -v quiet 2>&1 | Select-Object -Last 2
if ($LASTEXITCODE -ne 0) { "BUILD FAILED"; exit 1 }
Get-Process Raven.Server,QAWorkloadClient -EA SilentlyContinue | Stop-Process -Force -EA SilentlyContinue
Start-Sleep -Milliseconds 1500
$base = "D:\temp\ravendb-24528"; New-Item -ItemType Directory -Force $base | Out-Null
$qa = "D:\workspace\ravendb-qa-workload-client\QAWorkloadClient\bin\Release\net8.0"
$qa1 = "$base\qa-slot1"; $qa2 = "$base\qa-slot2"
foreach ($q in @($qa1,$qa2)) { if (Test-Path $q) { Remove-Item $q -Recurse -Force }; Copy-Item $qa $q -Recurse }
Remove-Item "$base\progress.html" -EA SilentlyContinue
$runs = @(
  @{key='Auto';       mode='Auto';   queue=$null; iters=1;  seed=200},
  @{key='FileIo';     mode='FileIo'; queue=$null; iters=3; seed=500},
  @{key='Mmap';       mode='Mmap';   queue=$null; iters=3; seed=500},
  @{key='IoRing';     mode='IoRing'; queue=$null; iters=3; seed=500},
  @{key='IoRing-256'; mode='IoRing'; queue='256'; iters=3; seed=500}
)
$rows = $runs | ForEach-Object { [pscustomobject]@{ Key=$_.key; Mode=$_.mode; Queue=$_.queue; Selected=$null; Phase='pending'; IterTotal=$_.iters; IterDone=0; Passed=0; Failed=0; LastDelta=0; IndexErrors=0; LogErrors=0; Note=$null } }
ConvertTo-Json @($rows) -Depth 3 | Set-Content "$base\progress.json"
function Run-Mode($m,$port,$qaDir,$out) {
  $a = @('run','--no-build','-c','Release','--project','test/Tryouts','--','carscenario','--mode',$m.mode,'--url',"http://127.0.0.1:$port",'--qa-dir',$qaDir,'--iterations',"$($m.iters)",'--load-seconds','30','--seed-docs',"$($m.seed)")
  if ($m.queue) { $a += @('--queue',$m.queue) }
  Start-Process dotnet -ArgumentList $a -WorkingDirectory $repo -RedirectStandardOutput $out -RedirectStandardError "$out.err" -PassThru -WindowStyle Hidden
}
(Run-Mode $runs[0] 8080 $qa  "$base\log-Auto.txt").WaitForExit()
@((Run-Mode $runs[1] 8081 $qa1 "$base\log-FileIo.txt"),(Run-Mode $runs[2] 8082 $qa2 "$base\log-Mmap.txt")) | ForEach-Object { $_.WaitForExit() }
@((Run-Mode $runs[3] 8081 $qa1 "$base\log-IoRing.txt"),(Run-Mode $runs[4] 8082 $qa2 "$base\log-IoRing-256.txt")) | ForEach-Object { $_.WaitForExit() }
$log = "$repo\test\RunBooks\RavenDB-24528\windows-carmatrix.log"
"==== Windows car matrix (parallel x2) $(Get-Date -Format o) ====" | Out-File $log
Get-ChildItem "$base\log-*.txt" | ForEach-Object { "`n===== $($_.BaseName) =====" | Add-Content $log; Get-Content $_.FullName | Select-String "selected WriteMode|exit=|seeded |during load|INDEX PROBLEM|recovery log:|reclog|RECOVERY FAILED|attempt |iteration .*:|== carscenario summary" | Add-Content $log }
"`n==== matrix done $(Get-Date -Format o) ====" | Add-Content $log
"DONE"
