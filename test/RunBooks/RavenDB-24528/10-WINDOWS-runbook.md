# RavenDB-24528 - Windows Runbook

Read `00-REFERENCE.md` first. Tick boxes as you go; fill the Result column with selected mode + crash iterations + integrity outcome + any error text.

Environment: Windows 11, RavenDB v8.0 build from this repo. Crash tool: `Stop-Process -Force` (or System Informer Terminate).

---

## Pre-flight

- [x] `dotnet build` succeeds (built via `test/Tryouts/Tryouts.csproj -c Release`, which builds the server).
- [x] Server binary exists: `src\Raven.Server\bin\Release\net10.0\Raven.Server.exe`.
- [x] Build identity (server banner): `8.0.0-custom-80, Commit a377982`. (Session HEAD: 4b860fdacfc - note if these should match.)
- [ ] QA client built: `cd D:\workspace\ravendb-qa-workload-client && dotnet build -c Release`; `Databases.zip` extracted to `QAWorkloadClient\Databases\`. (Dataset extracted: RookDB-TMI-CORE-PROD, RookDB-TMI-PROD. Client build/compat still pending.)
- [ ] QA client compat pre-flight passed (config + small deployDocuments writes to the 8.0 dev server). If failed, note workaround used: ____________
- [x] Sanity / orchestrator smoke: `node-info` command works; full `scenario` cycle (start->load->crash->recover->export/import) validated against a live server.

---

## Config-took-effect sanity (guards Program.cs ordering / configure-once)

- [ ] Start server `--Storage.WriteMode=Mmap`. node-info reports `Mmap(4)`. (If it reports anything else, the config did not take effect - STOP and file a bug.) Result: ____________

---

## Part 1 - WriteMode (each: configure -> start -> confirm mode -> workload -> hard-kill x25 -> recover -> export+import integrity)

For automated torture use: `dotnet run -c Release --project test/Tryouts -- scenario --mode <MODE> --iterations 25 --load-seconds 30`. For the realistic dataset, drive the QA client and crash manually per section 6/7 of the reference.

- [ ] **Auto** - expect node-info `IoRing(3)`. Workload + 25x crash + integrity all pass. Result (1-iter via fallback test confirms Auto resolves; full 25x pending): ____________
- [ ] **FileIo** - expect `FileIo(2)`. Workload + 25x crash + integrity pass. Result (Auto+(-1) resolved to FileIo + integrity MATCH for 1 iter; explicit FileIo 25x pending): ____________
- [~] **Mmap** - expect `Mmap(4)`. 1-iter harness: node-info=Mmap, ~645k docs, crash, recover, export/import MATCH (656151=656151). Full 25x pending. Result: 1-iter PASS
- [~] **IoRing** (default queue 1024) - 1-iter harness: node-info=IoRing, ~662k docs, crash, recover, export/import MATCH (674401=674401). Full 25x pending. Result: 1-iter PASS
- [x] **VectoredFileIo** - **D1 CONFIRMED FAIL-TO-START.** `TypeInitializationException -> NotSupportedException: "Failed to configure PAL library with 'VectoredFileIo' write mode. Arch: X64, OSDesc: Microsoft Windows 10.0.26200. Errno: 50='The request is not supported.'"` via `PalHelper.ThrowLastError` (PalHelper.cs:57). Server exits (code 0xE0434352).
  - [x] Decision: ticket bug - FIXED. RavenDB-24528 matrix updated (VectoredFileIo split: Linux=supported, Windows=must fail-to-start).

---

## Part 2 - IoRingQueueSize (Windows primary)

- [x] **queue=-1, WriteMode=Auto** - CONFIRMED start + fallback to `FileIo(2)` (node-info=FileIo, not IoRing), ~661k docs, crash, recover, export/import MATCH (672601=672601). The ticket's "-1 disables + falls back" behavior works - but ONLY under Auto. TODO: confirm a log line names the io_ring-disabled fallback.
- [x] **queue=-1, WriteMode=IoRing** - **D2 CONFIRMED FAIL-TO-START** (NOT the ticket's "must start + fall back"). `NotSupportedException: "Failed to configure PAL library with 'IoRing' write mode. ... Errno: 50='The request is not supported.' (rc=0). FailCode=FailCreateIoRing."` Server exits.
  - [x] Decision: RavenDB-26859 - keep failing for explicit non-Auto + (-1), but with a clear message; clarify config doc that -1 disables IoRing under Auto. Root cause: `rvn_one_time_init` explicit io_ring case returns the init failure directly (no fallback); fallback exists only under Auto.
- [x] **queue=0, WriteMode=IoRing** - **D3 FINDING: server STARTED in IoRing (did NOT fail).** Ticket requires fail-to-start with a specific invalid-config error. Reality: Windows `CreateIoRing(...,0,0,...)` accepts 0; server reports `IoRing(3)` and is fully functional: under load ~661k docs, crash, recover, export/import MATCH (672151=672151). So no corruption, but invalid config is silently accepted (config-validation gap). No C#-side Min/Max on IoRingQueueSize. -> RavenDB-26860: validate `-1` or `>= 3` and fail with a clear error (Linux parity; `CreateIoRing` is called with the value verbatim, `QueryIoRingCapabilities` loaded-but-unused, so effective size is kernel-chosen).
- [ ] **queue=256, WriteMode=IoRing** - expect `IoRing(3)`; full workload + 25x crash + integrity pass. Result: ____________
- [~] **queue=1024 (default), WriteMode=IoRing** - baseline; 1-iter PASS (see Part 1 IoRing). Full 25x pending. Result: 1-iter PASS
- [ ] **queue=2, WriteMode=IoRing** (boundary) - Windows has no min-3 check; expect `CreateIoRing(2)` -> verify start/fail + integrity if started. Result: ____________

---

## Extended (heavy profile - early bug discovery)

- [ ] **Crash-torture depth**: confirm 25+ kill/restart/integrity iterations completed for IoRing(1024), IoRing(256), Mmap, FileIo, Auto (above). Any single corruption = stop + file bug. Result: ____________
- [ ] **Kill-timing variation** (IoRing): kill (a) during heavy insert, (b) during index build, (c) during a flush/sync window. Each recovers + integrity passes. Result: ____________
- [ ] **Encryption-at-rest** (one IoRing cycle): create encrypted db, run workload, crash, recover, integrity. Result: ____________
- [ ] **Large + small doc mix** (one IoRing cycle): page-boundary stress (mix of >32KB blobs and many tiny docs), crash, recover, integrity. Result: ____________
- [ ] **Config precedence**: settings.json vs `--Storage.WriteMode` CLI vs `RAVEN_Storage_WriteMode` env each yield the same node-info mode. Result: ____________

---

## Results summary

| Area | Pass/Fail | Findings / bug links |
| --- | --- | --- |
| Part 1 Mmap | 1-iter PASS | integrity MATCH; 25x torture pending |
| Part 1 IoRing (1024) | 1-iter PASS | core write path OK, integrity MATCH; 25x pending |
| Part 1 Auto / FileIo | partial | Auto+(-1)->FileIo confirmed; explicit/default 25x pending |
| D1 VectoredFileIo | FINDING - FIXED | fail-to-start, NotSupported Errno 50; RavenDB-24528 matrix corrected (Windows=not supported) |
| D2 IoRing+(-1) | FINDING - filed | fail-to-start (not fallback); fallback only under Auto -> RavenDB-26859 |
| D3 IoRing+0 | FINDING - filed | silently starts IoRing + works; invalid config accepted -> RavenDB-26860 |
| Part 2 queue 256 / 25x torture / encrypt / timing / precedence | pending |  |

Initial automated findings: 2026-06-23, harness 1-iter each. Windows 10.0.26200, build 8.0.0-custom-80.
Overall Windows verdict: ____________  Date/tester: ____________
