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

## Config-took-effect sanity

- [ ] Start server `--Storage.WriteMode=Mmap`. node-info reports `Mmap(4)`. (Else config didn't apply - STOP, file bug.) Result: ____________

---

## Part 1 - WriteMode (each: configure -> start -> confirm mode -> workload -> hard-kill x25 -> recover -> export+import integrity)

Automated: `dotnet run -c Release --project test/Tryouts -- scenario --mode <MODE> --iterations 25 --load-seconds 30`. Realistic dataset: drive QA client + `kill -9` manually.

- [ ] **Auto** - expect `IoRing(3)` if kernel>=5.10 else `VectoredFileIo(1)`. Record which. Workload + 25x crash + integrity pass. Result: ____________
- [ ] **FileIo** - expect `FileIo(2)`. Workload + 25x crash + integrity pass. Result: ____________
- [ ] **VectoredFileIo** - expect `VectoredFileIo(1)` (supported on Linux, unlike Windows D1). Workload + 25x crash + integrity pass. Result: ____________
- [ ] **Mmap** - expect `Mmap(4)`. Workload + 25x crash + integrity pass. Result: ____________
- [ ] **IoRing** (default queue 1024) - **D4: kernel>=5.10 -> `IoRing(3)` + full cycle; kernel<5.10 -> FAIL-TO-START** (explicit IoRing does not fall back). Record kernel + outcome + any error: ____________
  - [ ] If failed on old kernel: paste verbatim error. If started: confirm workload + 25x crash + integrity pass.

---

## Part 2 - IoRingQueueSize (ticket marks this Windows-only; run this short Linux variant anyway)

Only meaningful when the kernel supports io_uring. If kernel<5.10, note all IoRing rows fail-to-start and skip.

- [ ] **queue=-1, WriteMode=Auto** - expect start + fallback to `VectoredFileIo(1)` (node-info != IoRing); log indicates io_ring disabled. Result: ____________
- [ ] **queue=-1, WriteMode=IoRing** - expect FAIL-TO-START (explicit IoRing, no fallback). `... -- negative --mode IoRing --queue -1`. Paste error: ____________
- [ ] **queue=0, WriteMode=IoRing** - expect FAIL-TO-START, native `ENOSPC` (queue<3). `... -- negative --mode IoRing --queue 0`. Does the message name the bad setting or is it generic? ____________
- [ ] **queue=2, WriteMode=IoRing** - expect FAIL-TO-START, `ENOSPC` (queue<3 boundary). Paste error: ____________
- [ ] **queue=3, WriteMode=IoRing** - expect start `IoRing(3)` (min allowed). Workload + crash + integrity. Result: ____________
- [ ] **queue=256, WriteMode=IoRing** - expect `IoRing(3)`; full workload + 25x crash + integrity. Result: ____________
- [ ] **queue=1024 (default), WriteMode=IoRing** - baseline; full workload + 25x crash + integrity. Result: ____________

---

## Extended (heavy profile)

- [ ] **Crash-torture depth**: 25+ kill/restart/integrity iterations for Auto, IoRing, VectoredFileIo, Mmap, FileIo. Any corruption = stop + file bug. Result: ____________
- [ ] **Kill-timing variation** (IoRing or VectoredFileIo): kill during (a) heavy insert, (b) index build, (c) flush/sync window. Recover + integrity each. Result: ____________
- [ ] **Encryption-at-rest** (one cycle, primary mode): encrypted db, workload, crash, recover, integrity. Result: ____________
- [ ] **Large + small doc mix** (one cycle): page-boundary stress, crash, recover, integrity. Result: ____________
- [ ] **Config precedence**: settings.json vs `--Storage.WriteMode` CLI vs `RAVEN_Storage_WriteMode` env yield the same mode. Result: ____________
- [ ] **memlock stress** (optional): lower `ulimit -l`, start IoRing, observe whether the OOM path triggers the clear `prlimit` guidance from `Pager.cs`. Result: ____________

---

## Results summary

| Area | Pass/Fail | Findings / bug links |
| --- | --- | --- |
| Part 1 (Auto/FileIo/VectoredFileIo/Mmap) |  |  |
| D4 IoRing on Linux (kernel ____) |  |  |
| Part 2 queue sizes (Linux variant) |  |  |
| Extended (torture/encrypt/timing/memlock) |  |  |

Overall Linux verdict: ____________  Kernel: ____________  Date/tester: ____________
