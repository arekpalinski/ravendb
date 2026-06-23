# RavenDB-24528 - Shared Reference (read first)

Ticket: https://issues.hibernatingrhinos.com/issue/RavenDB-24528
Scope: validate `Storage.WriteMode` and `Storage.IoRingQueueSize` for functional correctness, data integrity across a hard crash, and graceful platform handling on Windows + Linux.

This file is OS-agnostic. Execute `10-WINDOWS-runbook.md` on Windows and `20-LINUX-runbook.md` on Linux. Both reference the procedures and the behavior tables here.

---

## 1. The settings under test

| Setting | Type | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `Storage.WriteMode` | enum `RvnWriteMode` | `Auto` | ServerWideOnly | Auto/VectoredFileIo/FileIo/IoRing/Mmap |
| `Storage.IoRingQueueSize` | int | `1024` | ServerWideOnly | `-1` disables io_ring; native-validated only (no C# Min/Max) |

`ServerWideOnly` = must be set at the server level (settings.json / CLI / env), not per-database. The value is process-global and applied once at process startup.

### RvnWriteMode enum mapping (native rvn.h matches C# Pal.cs exactly)

| Index | Name |
| --- | --- |
| 0 | Auto |
| 1 | VectoredFileIo |
| 2 | FileIo |
| 3 | IoRing |
| 4 | Mmap |

---

## 2. How to set the config

Three equivalent ways (CLI overrides settings.json overrides defaults; env var also works):

settings.json (in the server output dir):
```json
{
  "Storage.WriteMode": "IoRing",
  "Storage.IoRingQueueSize": 256,
  "Security.UnsecuredAccessAllowed": "PublicNetwork",
  "Setup.Mode": "None",
  "License.Eula.Accepted": true
}
```

CLI args:
```
--Storage.WriteMode=IoRing --Storage.IoRingQueueSize=256
```

Env (Windows PowerShell): `$env:RAVEN_Storage_WriteMode = "IoRing"`
Env (Linux bash): `export RAVEN_Storage_WriteMode=IoRing`

The Tryouts orchestrator (section 8) sets these via CLI args.

---

## 3. Observe the actually-selected write mode

Endpoint `GET /cluster/node-info` returns a `WriteMode` field (added by PR 21031). Reachable unauthenticated on a dev server with `Security.UnsecuredAccessAllowed=PublicNetwork`.

The field is `WriteMode`; value is the enum name (e.g. `"IoRing"`) or the integer index from the table above. Check both.

Windows (PowerShell):
```powershell
(Invoke-RestMethod http://127.0.0.1:8080/cluster/node-info).WriteMode
```
Linux (bash):
```bash
curl -s http://127.0.0.1:8080/cluster/node-info | python3 -c "import sys,json;print(json.load(sys.stdin).get('WriteMode'))"
```
Quick check via Tryouts: `dotnet run -c Release --project test/Tryouts -- node-info http://127.0.0.1:8080`

If the server fails to start, this endpoint is unreachable - that itself is the "fail to start" signal; capture the console/log output instead.

---

## 4. Verified code-behavior reference (the expected results)

Source: `Raven.Pal/src/{win,posix}/ioring.c`, `shared_all.c`, `Sparrow.Server/Platform/Pal.cs`. All errors surface at server process startup (first Voron env open).

### Resolver `rvn_one_time_init`

Windows: cases `IoRing`, `Auto`, `FileIo`, `Mmap`. There is NO `VectoredFileIo` case -> hits `default:` -> `FAIL_INVALID_CONFIGURATION`.
Linux: cases `IoRing`, `Auto`, `VectoredFileIo`, `FileIo`, `Mmap`.
- `Auto`: try io_ring init; success -> IoRing; failure -> fall through (Windows -> FileIo; Linux -> VectoredFileIo).
- Explicit `IoRing`: returns the init result directly. NO fallback. Init failure -> server fails to start.

### io_ring init queue-size validation

| Platform | queue < 0 | queue == 0 | 0 < queue < 3 | queue >= 3 | other reqs |
| --- | --- | --- | --- | --- | --- |
| Windows | disabled (ENOTSUP) | -> `CreateIoRing(...,0,0,...)` (outcome unverified) | -> CreateIoRing | -> CreateIoRing | kernel32 io_ring fns must load |
| Linux | disabled (ENOTSUP) | ENOSPC error | ENOSPC error | io_uring_queue_init | kernel >= 5.10, 64-bit |

"disabled" matters only under `Auto` (-> fallback). Under explicit `IoRing`, "disabled"/error -> fail to start.

### Expected outcome matrix

| WriteMode | IoRingQueueSize | Windows | Linux |
| --- | --- | --- | --- |
| Auto | 1024 | IoRing(3) | IoRing(3) if kernel>=5.10 else VectoredFileIo(1) |
| Auto | -1 | FileIo(2) (fallback) | VectoredFileIo(1) (fallback) |
| FileIo | any | FileIo(2) | FileIo(2) |
| VectoredFileIo | any | FAIL-TO-START (D1) | VectoredFileIo(1) |
| Mmap | any | Mmap(4) | Mmap(4) |
| IoRing | 1024/256 | IoRing(3) | IoRing(3) if kernel>=5.10 else FAIL-TO-START (D4) |
| IoRing | -1 | FAIL-TO-START (D2) | FAIL-TO-START |
| IoRing | 0 | verify (D3) | FAIL-TO-START (ENOSPC) |
| IoRing | 2 | verify (CreateIoRing(2)) | FAIL-TO-START (ENOSPC) |

---

## 5. Discrepancy watch-list (ticket vs code - confirm and capture evidence)

| ID | Scenario | Ticket says | Code does | Status / action |
| --- | --- | --- | --- | --- |
| D1 | VectoredFileIo on Windows | supported ("Both") | fails to start (no switch case) | Windows CONFIRMED + ticket FIXED (matrix split). Linux: VectoredFileIo IS supported - verify it works. |
| D2 | IoRing + queue=-1 | "must start but fall back" | fails to start (no fallback for explicit IoRing) | Windows CONFIRMED -> **RavenDB-26859**. Linux: verify explicit IoRing+(-1) fails and Auto+(-1) falls back to VectoredFileIo. |
| D3 | IoRing + queue=0 (Win) | "fail to start, specific error about invalid config" | Windows CreateIoRing(0) accepts it, silently starts IoRing (effective size kernel-chosen) | Windows CONFIRMED (no fail/error) -> **RavenDB-26860**. Linux: queue<3 already fails (ENOSPC) - verify divergence. |
| D4 | IoRing on Linux | "should error or fall back" | kernel>=5.10 -> succeeds (io_uring); else fail (no fallback) | OPEN - Linux only. Record `uname -r`; assert actual behavior (likely just works on modern kernels). |

When an observed result differs from the ticket, that is a finding, not a runbook failure. Record actual behavior + evidence; we decide bug-vs-spec afterward.

---

## 6. Hard-crash procedure

Run load (section 7), then hard-kill the server process mid-write, restart, let it recover, then run the integrity check (section 9).

Get the PID: from `Get-Process Raven.Server` (Win) / `pgrep -f Raven.Server` (Linux), or the orchestrator prints it.

Windows: `Stop-Process -Id <pid> -Force` (equivalent to System Informer "Terminate"). Or `taskkill /F /PID <pid>`.
Linux: `kill -9 <pid>`.

NOTE: process kill validates Voron journal recovery, NOT OS-level fsync durability (fsync'd data survives a process kill because the OS already flushed it). True power-loss (VM reset) is out of scope for this runbook.

Restart with the SAME `--DataDir` and SAME WriteMode/queue settings. Wait until databases load (poll node-info / DB stats) before the integrity check.

---

## 7. Load - "Car dealership" workload (QA Workload Client)

Repo (Windows): `D:\workspace\ravendb-qa-workload-client`. Linux: location differs - discover it first (`find ~ -maxdepth 4 -iname QAWorkloadClient.csproj 2>/dev/null` or check the clone dir).

Prereqs:
1. Extract `QAWorkloadClient/Databases.zip` -> `QAWorkloadClient/Databases/` (contains `RookDB-TMI-PROD`, `RookDB-TMI-CORE-PROD`). The `Databases/` folder is NOT in git; it must be extracted.
2. Build: `dotnet build -c Release` (net8.0; references RavenDB.Client 7.0.0).

Compat pre-flight (RUN BEFORE relying on it): point the client at the 8.0 dev server with `config` + a small `deployDocuments`, confirm docs land. If 7.0->8.0 incompatibility breaks it, either bump `RavenDB.Client` to 8.0 in `QAWorkloadClient.csproj` and rebuild, or use the orchestrator's in-harness load generator instead.

Workflow (heavy profile):
```
QAWorkloadClient config --urls "http://127.0.0.1:8080"
QAWorkloadClient dca -p "<repo>/QAWorkloadClient/Databases/RookDB-TMI-CORE-PROD/CustomAnalyzer.cs" -n "Rook.RavenAnalyzers.ASCIIAnalyzer"
QAWorkloadClient di -db RookDB-TMI-PROD
QAWorkloadClient di -db RookDB-TMI-CORE-PROD
QAWorkloadClient deployDocuments -n 2000 -db RookDB-TMI-PROD
QAWorkloadClient deployDocuments -n 2000 -db RookDB-TMI-CORE-PROD
QAWorkloadClient ro -th 25 -mincs 50000 -maxcs 100000 -db RookDB-TMI-PROD   # run sustained
QAWorkloadClient rq -th 25 -db RookDB-TMI-PROD                              # in parallel
```
Windows exe: `QAWorkloadClient.exe ...`. Linux: `dotnet QAWorkloadClient.dll ...` or `./QAWorkloadClient ...` from the build output.

The orchestrator (section 8) uses its own in-harness concurrent CRUD generator for the unattended 25x torture loop (no dataset/compat dependency). Use the QA client for the realistic-dataset manual runs.

---

## 8. Tryouts orchestrator

File: `test/Tryouts/Program.cs`. Build with `dotnet build RavenDB.sln -c Release` (builds the server too). Run from repo root.

Auto-discovers the server at `src/Raven.Server/bin/Release/net10.0/Raven.Server(.exe)`; override with `RAVEN_SERVER_PATH`.

Commands:
```
# Quick: print selected WriteMode from a running server
dotnet run -c Release --project test/Tryouts -- node-info http://127.0.0.1:8080

# Full crash+integrity torture loop for one config
dotnet run -c Release --project test/Tryouts -- scenario --mode IoRing --queue 1024 --iterations 25 --load-seconds 30

# Negative config: expect fail-to-start, capture the error
dotnet run -c Release --project test/Tryouts -- negative --mode VectoredFileIo
dotnet run -c Release --project test/Tryouts -- negative --mode IoRing --queue -1
dotnet run -c Release --project test/Tryouts -- negative --mode IoRing --queue 0

# Standalone integrity check against a running server+db
dotnet run -c Release --project test/Tryouts -- integrity --url http://127.0.0.1:8080 --db <name>
```
Each `scenario` iteration: start server -> assert mode via node-info -> concurrent CRUD for N seconds -> hard kill -> restart same data dir -> wait recovery -> Smuggler export to .ravendump -> import into fresh db -> assert both succeed + doc counts match. Logs PASS/FAIL per iteration and a final summary.

---

## 9. Integrity check (per ticket)

Pass iff BOTH export and import complete with zero errors.

Option A - orchestrator: built into `scenario`, or run `integrity` standalone.
Option B - Studio: Databases > (db) > Settings > Export Database -> .ravendump; create a new empty db; Import the file. No errors on either step.
Option C - client API: `store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), filePath)` then `ImportAsync(new DatabaseSmugglerImportOptions(), filePath)` into a new db.

Extra assertion (all options): compare collection doc counts source vs imported.

---

## 10. Pass / Fail criteria

A scenario PASSES when:
- The server reaches the expected state (selected mode matches the expectation table in section 4, OR fails to start exactly where the table says FAIL-TO-START).
- After every hard crash, the server recovers and starts.
- Every integrity check (export + import) completes with zero errors and matching counts.

A scenario FAILS (record + file bug) when:
- Selected mode != expected and != a documented discrepancy.
- Server fails to recover after a crash, or recovery throws.
- Export or import errors, or counts diverge.
- Any data corruption, missing documents, or Voron assertion.

Discrepancies D1-D4: record ACTUAL behavior with verbatim evidence; do not auto-fail.

---

## 11. Recording results

In each OS runbook, tick the checkbox and fill the result cell with: selected mode, crash iterations survived, integrity result, and paste any error text. For fail-to-start cases, paste the full exception/log lines (these are the deliverable for D1-D4).

File bugs as subtasks of RavenDB-24528 with: OS + kernel/build, exact settings, command, observed vs expected, evidence.
