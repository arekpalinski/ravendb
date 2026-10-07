# RavenDB-24528 - Linux Runbook

Read `00-REFERENCE.md` first. Tick boxes as you go; fill Result cells with selected mode + crash iterations + integrity outcome + any error text.

Environment: Ubuntu, RavenDB v8.0 build from this repo. Crash tool: `kill -9`.

---

## Pre-flight

- [ ] Record kernel: `uname -r` = ____________ (io_ring/io_uring requires kernel >= 5.10, 64-bit). This determines IoRing/Auto expectations (D4).
- [ ] Record `ulimit -l` (memlock) = ____________ . io_uring needs locked memory; if io_ring init fails with an out-of-memory error, raise it: `sudo prlimit --memlock=unlimited:unlimited --pid $$` then relaunch.
- [ ] `dotnet build RavenDB.sln -c Release` succeeds.
- [ ] Server binary exists: `src/Raven.Server/bin/Release/net10.0/Raven.Server` (no .exe on Linux).
- [ ] Record build/commit: `git rev-parse --short HEAD` = ____________
- [ ] **Locate the QA client** (path differs from Windows): `find ~ -maxdepth 5 -iname QAWorkloadClient.csproj 2>/dev/null` -> ____________ . Build: `dotnet build -c Release`. Extract `Databases.zip` -> `QAWorkloadClient/Databases/`.
- [ ] QA client compat pre-flight passed (config + small deployDocuments writes to the 8.0 server). Workaround if needed: ____________
- [ ] Orchestrator smoke: `dotnet run -c Release --project test/Tryouts -- node-info http://127.0.0.1:8080` prints the mode.

---

## Linux setup notes (READ FIRST - differs from Windows)

- **Orchestrator defaults are hardcoded Windows paths - override on Linux:**
  - `carscenario` QA dir: pass `--qa-dir <abs path to QAWorkloadClient/bin/Release/net8.0>` or set `QA_CLIENT_DIR`.
  - `numbers-seed`/`numbers-scenario` indexes dump: pass `--indexes <abs path to Numbers_and_units-Indexes.ravendbdump>`.
  - Server auto-discovers `src/Raven.Server/bin/Release/net10.0/Raven.Server`; override with `RAVEN_SERVER_PATH`. Artifacts go under `$TMPDIR/ravendb-24528` automatically (not D:\temp).
- **Files NOT in this repo - copy to the Linux box first:**
  - `Numbers_and_units-Indexes.ravendbdump` (loose file, was `D:\workspace\ravendb-dumps\` on Windows; not version-controlled) - needed for the Numbers dataset.
  - car-dealership `Databases.zip` ships in the QA-client repo; extract into its `bin/Release/net8.0/Databases/` (NOT the source tree - the analyzer `.cs` break the build).
- **PowerShell launchers are Windows-only** (`run-windows-matrix.ps1`). On Linux run modes individually (`dotnet run ... carscenario --mode X ...`) or a small bash loop; the orchestrator commands themselves are cross-platform. The agent background-task time cap was a Windows-tooling limit, not a Linux concern - a terminal `dotnet run` has no such cap.
- **Encryption on Linux**: build the server with `dotnet build src/Raven.Server -c Release -p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP` (the over-HTTP allowance is compile-time). DPAPI is Windows-only, so the master key differs - if `carscenario --encrypt` fails to register the secret key, configure `Security.MasterKeyPath` (a key file). Verify on the box.

---

## Config-took-effect sanity

- [ ] Start server `--Storage.WriteMode=Mmap`. node-info reports `Mmap(4)`. (Else config didn't apply - STOP, file bug.) Result: ____________

---

## Part 1 - WriteMode (each: configure -> start -> confirm mode -> workload -> hard-kill x3 -> recover -> export+import integrity)

Automated (synthetic load): `dotnet run -c Release --project test/Tryouts -- scenario --mode <MODE> --iterations 3 --load-seconds 30`.
Automated (realistic car-dealership load): `... -- carscenario --mode <MODE> --iterations 3 --load-seconds 30 --seed-docs 500 --qa-dir <QA bin>` - build the QA client first and extract `Databases.zip` into its `bin/Release/net8.0/`; set `QA_CLIENT_DIR` or pass `--qa-dir` (see REFERENCE 7/8). Or drive the QA client + `kill -9` manually.

- [ ] **Auto** - expect `IoRing(3)` if kernel>=5.10 else `VectoredFileIo(1)`. Record which. Workload + 3x crash + integrity pass. Result: ____________
- [ ] **FileIo** - expect `FileIo(2)`. Workload + 3x crash + integrity pass. Result: ____________
- [ ] **VectoredFileIo** - expect `VectoredFileIo(1)` (supported on Linux, unlike Windows D1). Workload + 3x crash + integrity pass. Result: ____________
- [ ] **Mmap** - expect `Mmap(4)`. Workload + 3x crash + integrity pass. Result: ____________
- [ ] **IoRing** (default queue 1024) - **D4: kernel>=5.10 -> `IoRing(3)` + full cycle; kernel<5.10 -> FAIL-TO-START** (explicit IoRing does not fall back). Record kernel + outcome + any error: ____________
  - [ ] If failed on old kernel: paste verbatim error. If started: confirm workload + 3x crash + integrity pass.

---

## Part 2 - IoRingQueueSize (ticket marks this Windows-only; run this short Linux variant anyway)

Only meaningful when the kernel supports io_uring. If kernel<5.10, note all IoRing rows fail-to-start and skip.

- [ ] **queue=-1, WriteMode=Auto** - expect start + fallback to `VectoredFileIo(1)` (node-info != IoRing); log indicates io_ring disabled. Result: ____________
- [ ] **queue=-1, WriteMode=IoRing** - expect FAIL-TO-START (explicit IoRing, no fallback). `... -- negative --mode IoRing --queue -1`. Paste error: ____________
- [ ] **queue=0, WriteMode=IoRing** - expect FAIL-TO-START, native `ENOSPC` (queue<3). `... -- negative --mode IoRing --queue 0`. Does the message name the bad setting or is it generic? ____________
- [ ] **queue=2, WriteMode=IoRing** - expect FAIL-TO-START, `ENOSPC` (queue<3 boundary). Paste error: ____________
- [ ] **queue=3, WriteMode=IoRing** - expect start `IoRing(3)` (min allowed). Workload + crash + integrity. Result: ____________
- [ ] **queue=256, WriteMode=IoRing** - expect `IoRing(3)`; full workload + 3x crash + integrity. Result: ____________
- [ ] **queue=1024 (default), WriteMode=IoRing** - baseline; full workload + 3x crash + integrity. Result: ____________

---

## Extended (heavy profile)

- [ ] **Crash-torture depth**: 3+ kill/restart/integrity iterations for Auto, IoRing, VectoredFileIo, Mmap, FileIo. Any corruption = stop + file bug. Result: ____________
- [ ] **Kill-timing variation**: `carscenario --mode <M> --vary-kill --iterations 5 --qa-dir <bin>` (kills at 3/7/12/20/30s - early hits index-build/initial-flush, late = steady heavy-insert). Windows: 5/5 PASS. Result: ____________
- [ ] **Encryption-at-rest**: `carscenario --mode IoRing --encrypt --iterations 3 --qa-dir <bin>` - needs server built `-p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP` + RAVEN_LICENSE in env; Linux may also need `Security.MasterKeyPath` (DPAPI is Windows-only). Windows: 3/3 PASS. Result: ____________
- [ ] **Large + small doc mix** (one cycle): page-boundary stress, crash, recover, integrity. Result: ____________
- [ ] **Config precedence**: settings.json vs `--Storage.WriteMode` CLI vs `RAVEN_Storage_WriteMode` env yield the same mode. Result: ____________
- [ ] **memlock stress** (optional): lower `ulimit -l`, start IoRing, observe whether the OOM path triggers the clear `prlimit` guidance from `Pager.cs`. Result: ____________

---

## Numbers and units dataset (alternative dataset - collections Numbers + Users, ~10M docs + imported indexes)

Create once: `dotnet run -c Release --project test/Tryouts -- numbers-seed --count 5000000 --indexes <abs path to Numbers_and_units-Indexes.ravendbdump>` (5M Numbers {PartId,PriceValue,Count} + 5M Users {Date,Count} + import indexes).
Crash matrix per mode: `dotnet run -c Release --project test/Tryouts -- numbers-scenario --mode <M> --iterations 3 --seed-count 5000000 --load-seconds 20 --indexes <dump>` - per mode: seed 10M -> 3x [insert load -> hard-kill mid-write -> recover -> verify recovered>=baseline + index-state + recovery-log ERROR/FATAL]. Per-iteration uses count+index (export/import too slow per-iter at 10M). For the one deep check, run `integrity --db NumbersAndUnits` against a server started on a crashed `numbers-<mode>-def` data dir.

- [ ] **FileIo** - 3x, recovered>=baseline, 0 index/log. Result: ____________
- [ ] **Mmap** - 3x. Result: ____________
- [ ] **VectoredFileIo** (Linux-only) - 3x. Result: ____________
- [ ] **IoRing** (kernel>=5.10) - 3x. Result: ____________
- [ ] **Final export/import** on one crashed dataset: MATCH. Result: ____________

Windows reference: all modes 3/3 PASS; final export/import MATCH (14.5M docs).

---

## Results summary

| Area | Pass/Fail | Findings / bug links |
| --- | --- | --- |
| Part 1 (Auto/FileIo/VectoredFileIo/Mmap) |  |  |
| D4 IoRing on Linux (kernel ____) |  |  |
| Part 2 queue sizes (Linux variant) |  |  |
| Numbers/Units matrix + final export/import |  |  |
| Extended (torture/encrypt/timing/memlock) |  |  |

Overall Linux verdict: ____________  Kernel: ____________  Date/tester: ____________
