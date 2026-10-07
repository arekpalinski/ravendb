# R6 - RavenDB-27656: Voron stress tests, shared journals with process kills, 1024 indexes

The write-path stress scenarios from the RavenDB-27377 PR testing, rerun on the merged code (they last ran on the PR
head 37648c35cfc, Windows only), plus the new `b-shared-kill`. Background and the scenario descriptions:
D:\workspace\RavenDB-27377-playbook.md (not in git; this runbook has everything needed to run them).

## How a scenario runs

```
cd test/Tryouts
dotnet run -c <Debug|Release> --no-build -- <scenario> --seed <N> --minutes <M> [--dir <path>]
```

- Crash scenarios restart Tryouts as a child (`--child`) and kill it at a seeded moment; the parent verifies recovery.
- Verdict: the last line is `PASS [...]` or `FAIL [...]`, exit code 0/1, and `results-<scenario>-<seed>.json` in the work dir
  (default `%TEMP%/voron-stress/<scenario>-<seed>`, on Linux pass `--dir` on ext4). A FAIL keeps the evidence; the same seed replays it.
- "all N iterations were vacuous" is a FAIL too: the child never committed before the kill, so nothing was tested.
- Use a fresh seed per run (the date works, e.g. 20261008) and never run two crash scenarios on one disk at the same time.

## Windows (unattended, about 6 hours)

Build both configurations first: `dotnet build test/Tryouts/Tryouts.csproj -c Debug` and `-c Release`.

| Scenario | Build | Minutes |
|---|---|---|
| a-order-fuzz, a-fault-matrix, a-merger-pump | Debug | 15 each |
| b-recycle-kill, b-shared-kill | Debug | 15 each |
| b-tail-shapes | Release | 5 |
| b-prewarm-race | Release | 15 |
| c-reader-soak, c-flush-race | Release | 15 each |
| c-snapshot-model | Debug | 15 |
| d-page-integrity, d-drain-hammer | Debug | 15 each |
| d-flusher-liveness | Debug | 10 |
| e-crash-model, e-parent-churn, e-boundary-sweep | Debug | 15 each |
| f-span-diff | Debug | 15 |
| f-http-fuzz, f-compare-diff | Debug | seconds, no `--minutes` |
| g-cache-assert, g-wakeup-watchdog | Debug | 15 each |
| g-etag-restart | Release | 12 |
| h-tree-model, h-index-stress, h-deep-cursor | Debug | 15 each |
| h-compressed-churn | Release | 15 |
| d-avalanche | Release | 3, run alone (a measurement, not pass/fail) |

`h-tree-minimize` is a tool, not a test: skip it.

```powershell
$seed = (Get-Date -Format yyyyMMdd)
$runs = @(
  @('Debug','a-order-fuzz',15), @('Debug','a-fault-matrix',15), @('Debug','a-merger-pump',15),
  @('Debug','b-recycle-kill',15), @('Debug','b-shared-kill',15), @('Release','b-tail-shapes',5), @('Release','b-prewarm-race',15),
  @('Release','c-reader-soak',15), @('Release','c-flush-race',15), @('Debug','c-snapshot-model',15),
  @('Debug','d-page-integrity',15), @('Debug','d-drain-hammer',15), @('Debug','d-flusher-liveness',10),
  @('Debug','e-crash-model',15), @('Debug','e-parent-churn',15), @('Debug','e-boundary-sweep',15),
  @('Debug','f-span-diff',15), @('Debug','g-cache-assert',15), @('Debug','g-wakeup-watchdog',15), @('Release','g-etag-restart',12),
  @('Debug','h-tree-model',15), @('Debug','h-index-stress',15), @('Debug','h-deep-cursor',15), @('Release','h-compressed-churn',15))
New-Item -ItemType Directory -Force D:\temp\phase3-r6 | Out-Null
foreach ($r in $runs) {
  dotnet run --project test/Tryouts -c $r[0] --no-build -- $r[1] --seed $seed --minutes $r[2] --dir "D:\temp\phase3-r6\$($r[1])" *> "D:\temp\phase3-r6\$($r[1]).log"
  "$($r[1]) exit=$LASTEXITCODE" | Tee-Object -Append D:\temp\phase3-r6\summary.txt
}
```

Then `f-http-fuzz` and `f-compare-diff` (seconds each) and `d-avalanche` alone.

## Linux

The Linux-only code is what matters here: trickle writeback (`sync_file_range`), the queue-depth drain decision, the posix
rename + fsync of reused journals, journal zeroing pacing. Order (the playbook's Ubuntu track, no `io.max` throttling):

| Scenario | Build | Minutes |
|---|---|---|
| d-page-integrity, d-drain-hammer | Debug | 15 each |
| d-avalanche | Release | 3, alone |
| d-flusher-liveness | Debug | 10 |
| b-recycle-kill, b-shared-kill | Debug | 15 each |
| b-tail-shapes | Release | 5 |
| b-prewarm-race | Release | 15 |
| a-fault-matrix, a-order-fuzz | Debug | 15 each |
| c-reader-soak, c-flush-race | Release | 15 each |

```bash
seed=$(date +%Y%m%d); out=~/phase3-r6; mkdir -p $out
run() { dotnet run --project test/Tryouts -c "$1" --no-build -- "$2" --seed $seed --minutes "$3" --dir "$out/$2" > "$out/$2.log" 2>&1; echo "$2 exit=$?" | tee -a $out/summary.txt; }
run Debug d-page-integrity 15; run Debug d-drain-hammer 15; run Release d-avalanche 3; run Debug d-flusher-liveness 10
run Debug b-recycle-kill 15; run Debug b-shared-kill 15; run Release b-tail-shapes 5; run Release b-prewarm-race 15
run Debug a-fault-matrix 15; run Debug a-order-fuzz 15; run Release c-reader-soak 15; run Release c-flush-race 15
```

On a FAIL: `tar czf evidence-<scenario>-<seed>.tgz <work dir>`.

## 1024 indexes sharing journals (RavenDB-24069)

`StressTests/Issues/RavenDB_24069_Stress` builds a root and 1024 branch environments at the Voron level. Run it on both OSes:

```
dotnet test test/StressTests/StressTests.csproj -c Release --filter "FullyQualifiedName~RavenDB_24069_Stress"
```

Windows (NTFS, 1023 links per file): branches past the limit must fall back to their own journals. Linux ext4: all share.

## Expected results

- Every scenario ends with PASS and exit code 0; d-avalanche reports numbers only.
- b-shared-kill, per iteration: no acknowledged transaction of the root or a branch is lost, recovered hard-linked journals are
  read-only (done-writing), no `recyclable-journal.*` file in a branch folder, and the root's pool count matches the files on disk.
- RavenDB_24069_Stress passes.
