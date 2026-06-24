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

## Part 1 - WriteMode (each: configure -> start -> confirm mode -> workload -> hard-kill x3 -> recover -> export+import integrity)

For automated torture use: `dotnet run -c Release --project test/Tryouts -- scenario --mode <MODE> --iterations 3 --load-seconds 30`. For the realistic dataset, drive the QA client and crash manually per section 6/7 of the reference.

- [x] **Auto** - node-info=`IoRing(3)` confirmed. On Windows Auto = same write path as IoRing(1024), so NOT re-run in the car-dealership 3x matrix (redundant). Result: PASS (resolves to IoRing)
- [x] **FileIo** - node-info=`FileIo(2)`. Car-dealership 3x, FULL index set (PROD 100 / CORE-PROD 194): 3/3 PASS, integrity MATCH every iter (21144, 23882, ...), 0 index/log errors. Result: PASS (3/3)
- [x] **Mmap** - node-info=`Mmap(4)`. Car-dealership 3x, FULL index set: 3/3 PASS, integrity MATCH (21123, 24204, ...), 0 index/log errors. Result: PASS (3/3)
- [x] **IoRing** (default queue 1024) - node-info=`IoRing(3)`. Car-dealership 3x, indexes REDUCED to 15/DB (100/194-index reload was dominating recovery IO and not representative): 3/3 PASS, integrity MATCH (21486, 28540, 35871), 0 index/log errors. Result: PASS (3/3)
- [x] **VectoredFileIo** - **D1 CONFIRMED FAIL-TO-START.** `TypeInitializationException -> NotSupportedException: "Failed to configure PAL library with 'VectoredFileIo' write mode. Arch: X64, OSDesc: Microsoft Windows 10.0.26200. Errno: 50='The request is not supported.'"` via `PalHelper.ThrowLastError` (PalHelper.cs:57). Server exits (code 0xE0434352).
  - [x] Decision: ticket bug - FIXED. RavenDB-24528 matrix updated (VectoredFileIo split: Linux=supported, Windows=must fail-to-start).

---

## Part 2 - IoRingQueueSize (Windows primary)

- [x] **queue=-1, WriteMode=Auto** - CONFIRMED start + fallback to `FileIo(2)` (node-info=FileIo, not IoRing), ~661k docs, crash, recover, export/import MATCH (672601=672601). The ticket's "-1 disables + falls back" behavior works - but ONLY under Auto. TODO: confirm a log line names the io_ring-disabled fallback.
- [x] **queue=-1, WriteMode=IoRing** - **D2 CONFIRMED FAIL-TO-START** (NOT the ticket's "must start + fall back"). `NotSupportedException: "Failed to configure PAL library with 'IoRing' write mode. ... Errno: 50='The request is not supported.' (rc=0). FailCode=FailCreateIoRing."` Server exits.
  - [x] Decision: RavenDB-26859 - keep failing for explicit non-Auto + (-1), but with a clear message; clarify config doc that -1 disables IoRing under Auto. Root cause: `rvn_one_time_init` explicit io_ring case returns the init failure directly (no fallback); fallback exists only under Auto.
- [x] **queue=0, WriteMode=IoRing** - **D3 FINDING: server STARTED in IoRing (did NOT fail).** Ticket requires fail-to-start with a specific invalid-config error. Reality: Windows `CreateIoRing(...,0,0,...)` accepts 0; server reports `IoRing(3)` and is fully functional: under load ~661k docs, crash, recover, export/import MATCH (672151=672151). So no corruption, but invalid config is silently accepted (config-validation gap). No C#-side Min/Max on IoRingQueueSize. -> RavenDB-26860: validate `-1` or `>= 3` and fail with a clear error (Linux parity; `CreateIoRing` is called with the value verbatim, `QueryIoRingCapabilities` loaded-but-unused, so effective size is kernel-chosen).
- [x] **queue=256, WriteMode=IoRing** - node-info=`IoRing(3)`. Car-dealership 3x (15 indexes): 3/3 PASS, integrity MATCH (21511, 28401, 36166), 0 index/log errors. Result: PASS (3/3)
- [x] **queue=1024 (default), WriteMode=IoRing** - baseline; see Part 1 IoRing: 3/3 PASS (15 indexes). Result: PASS (3/3)
- [ ] **queue=2, WriteMode=IoRing** (boundary) - Windows has no min-3 check; expect `CreateIoRing(2)` -> verify start/fail + integrity if started. Result: ____________

---

## Extended (heavy profile - early bug discovery)

- [ ] **Crash-torture depth**: confirm 3+ kill/restart/integrity iterations completed for IoRing(1024), IoRing(256), Mmap, FileIo, Auto (above). Any single corruption = stop + file bug. Result: ____________
- [x] **Kill-timing variation** (IoRing, `carscenario --vary-kill`): 5 crashes at 3/7/12/20/30s into load - early (3s, +120 docs: index-build/initial-flush region) through steady heavy-insert (30s, +10486). 5/5 PASS, integrity MATCH at every kill point. No timing-sensitive recovery bug. Result: PASS (5/5)
- [x] **Encryption-at-rest** (IoRing, 15 idx, 3x): encrypted DBs created over HTTP (POST /admin/secrets 256-bit key + `Encrypted=true`; server built `-p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP`, license from RAVEN_LICENSE env). 3/3 PASS, integrity MATCH (20783, 27691, 35332), 0 index/ERROR/FATAL. Only WARN/recovery = benign Rachis leader-election timing (0.25s < 0.3s timeout) from the single-node bootstrap used for the secret-key setup - not encryption/data. Result: PASS (3/3)
- [ ] **Large + small doc mix** (one IoRing cycle): page-boundary stress (mix of >32KB blobs and many tiny docs), crash, recover, integrity. Result: ____________
- [x] **Config precedence**: all 3 sources set `Storage.WriteMode` (settings.json, env `RAVEN_Storage_WriteMode`, CLI `--Storage.WriteMode`). Precedence **CLI > settings.json > env** (CLI beats both; settings beats env). NOTE: env is lowest - settings.json overrides the env var (mildly surprising). Result: PASS

---

## Numbers and units dataset (alternative dataset - collections Numbers + Users, ~10M docs + imported indexes)

Created via `numbers-seed` (5M Numbers {PartId,PriceValue,Count} + 5M Users {Date,Count}; indexes from `Numbers_and_units-Indexes.ravendbdump`). Crash matrix via `numbers-scenario` - per mode: seed 10M, then 3x [insert load -> hard-kill mid-write -> recover -> verify recovered>=baseline + index-state + recovery-log ERROR/FATAL]. Per-iteration uses count+index (full export/import too slow per-iter on 10M); one deep export/import at the end.

- [x] **FileIo** - 3/3 PASS, recovered>=baseline every iter (10M->14.3M), 0 index/log errors.
- [x] **Mmap** - 3/3 PASS, recovered>=baseline (10M->13.9M), 0 index/log errors.
- [x] **IoRing** - 3/3 PASS, recovered>=baseline (10M->13.8M), 0 index/log errors.
- [x] **IoRing q256** - 3/3 PASS, recovered>=baseline (10M->14.5M), 0 index/log errors.
- [x] **Final export/import** (crashed IoRing-256 dataset, 14,513,026 docs): MATCH in 3:30 - deep integrity confirmed on a crash-recovered large dataset.

Result: all WriteModes PASS, zero data-integrity issues on the Numbers/Units dataset.

---

## Results summary

| Area | Pass/Fail | Findings / bug links |
| --- | --- | --- |
| FileIo (3x, full idx 100/194) | PASS 3/3 | integrity MATCH all iters; 0 index/log errors |
| Mmap (3x, full idx 100/194) | PASS 3/3 | integrity MATCH all iters; 0 index/log errors |
| IoRing 1024 (3x, 15 idx) | PASS 3/3 | integrity MATCH all iters; 0 index/log errors |
| IoRing 256 (3x, 15 idx) | PASS 3/3 | integrity MATCH all iters; 0 index/log errors |
| Auto | PASS | resolves to IoRing(3); not re-run (= IoRing path on Windows) |
| D1 VectoredFileIo | FINDING - FIXED | fail-to-start, NotSupported Errno 50; RavenDB-24528 matrix corrected (Windows=not supported) |
| D2 IoRing+(-1) | FINDING - filed | fail-to-start (fallback only under Auto) -> RavenDB-26859 |
| D3 IoRing+0 | FINDING - filed | silently starts IoRing, invalid config accepted -> RavenDB-26860 |
| Encryption-at-rest (IoRing, 3x) | PASS 3/3 | encrypted Voron survives crash; integrity MATCH; only benign Rachis election WARN |
| Kill-timing variation (IoRing, 3-30s) | PASS 5/5 | integrity MATCH at every kill point incl. 3s early kill |
| Config precedence | PASS | all 3 sources work; order CLI > settings.json > env (env lowest) |
| Numbers/Units matrix (FileIo/Mmap/IoRing/IoRing-256, 3x, 10M) | PASS 12/12 | recovered>=baseline all iters; 0 index/log errors |
| Numbers/Units final export/import (14.5M crashed) | MATCH | deep integrity on crash-recovered large dataset (3:30) |

Car-dealership 3x crash matrix completed 2026-06-24: all WriteModes PASS, zero data-integrity issues. FileIo/Mmap ran with the full index set (100/194 per DB); IoRing/IoRing-256 with indexes reduced to 15/DB (the 100-194 index reload dominated recovery IO and isn't representative - reducing it also confirmed indexes, not the write mode, were the recovery bottleneck). Earlier 1-iter findings + D1-D3: 2026-06-23. Windows 10.0.26200, build 8.0.0-custom-80.
Overall Windows verdict: ____________  Date/tester: ____________
