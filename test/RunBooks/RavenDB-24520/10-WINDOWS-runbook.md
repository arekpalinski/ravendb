# RavenDB-24520 Windows Runbook

Prereqs: `dotnet build src/Raven.Server -c Release` and `dotnet build test/Tryouts -c Release`. Read [00-REFERENCE.md](00-REFERENCE.md) first. All commands from repo root. **Never open a server on `golden/`.**

Legend: `[ ]` todo, `[x]` done (result noted inline / in `findings-scenario1.md`).

## Post-fix retest (RavenDB-27156 + RavenDB-27166)

Run this whenever the fixes change. Branch must contain the fix commits (verify: `git merge-base --is-ancestor 5e4f97e32d5 HEAD`).

- [x] **All fix tests green.** `dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~RavenDB_27156|FullyQualifiedName~RavenDB_27166"` -> 8/8 passed (~10 s).
- [x] **F-3 AV loop: 0 crashes.** Loop the e2e repro; pre-fix baseline was ~40-60% ACCESS_VIOLATION (exit `0xC0000005`):
      `dotnet test test/SlowTests -c Release --no-build --filter "FullyQualifiedName~RavenDB_27156_e2e.TornJournalWrite_OnSharedRoot"`
      -> **14/14 PASS, 0 AV** on this branch (2026-08-04). Fix confirmed on Windows.
- [ ] Scenario 1 corruption matrix re-run against the fixed build (below) - blast radius (F-1) expected unchanged, poisoning/unload behavior may differ.
- [ ] Real disk-full re-run on F: (below) - expected still graceful.

## Phase 0 - Baseline

- [ ] `dotnet run --project test/Tryouts -c Release --no-build -- seed 2`  (golden DB: 2 posts dumps + users, sampled 1/50)
- [ ] `... -- map D:\temp\24520\golden`  (confirm >=3 branch envs share journals; note JournalIds + which txs are unsynced - the active journal should hold fresh unsynced index txs)
- [ ] `... -- restore-work` then `... -- verify D:\temp\24520\work`  (uncorrupted baseline must report OK)

## Scenario 1 - corruption at rest

Each cell: `dotnet run --project test/Tryouts -c Release --no-build -- cell <name> <op> <ownerFilter> <which> [fileSelector]`.
A cell auto-does restore-work -> corrupt -> verify -> append a row to `D:\temp\24520\findings-scenario1.md`.
`ownerFilter` is an **exact** index dir name (e.g. `Questions_Tags`, not a prefix), `@SharedJournals`, `<link-record>`, or `any`.

### 1A - payload corruption (hash mismatch)
- [ ] `cell 1A-branch-unsynced payload Questions_Tags last shared`
- [ ] `cell 1A-synced-first payload Questions_Tags first inode:first`  (old journal, linked by 1 index -> expect only that index faults)
- [ ] `cell 1A-root payload @SharedJournals last shared`  (root's own tx -> worst blast radius)
- [ ] `cell 1A-other-env payload Users_Search first shared`  (corrupt env X's tx; does env Y in the same file still recover?)

### 1B - header marker smash (looks like end-of-journal)
- [ ] `cell 1B-marker-tail marker Users_Search last shared`  (expect clean - treated as EOF)
- [ ] `cell 1B-marker-mid marker Users_Search first shared`  (expect cascade)

### 1C - header field corruption
- [ ] `cell 1C-hash hash Questions_Search last shared`
- [ ] `cell 1C-txid txid Questions_Search last shared`
- [ ] `cell 1C-journalid journalid Questions_Search last shared`  (expect CLEAN load + silent tx drop -> F-2)

### 1D - block zeroing / truncation
- [ ] `cell 1D-zeroblock zero-block Questions_Search last shared`
- [ ] `cell 1D-trunc-tail truncate-tail any first shared`  (drop last 4KB)
- [ ] `cell 1D-trunc-mid truncate-mid Questions_Search last shared`

### 1E - link-record corruption
- [ ] `cell 1E-linkrec linkrecord <link-record> first shared`

### 1F - file-level topology
- [ ] `cell 1F-delete-active delete any first shared`  (expect clean - inode survives via branch links)
- [ ] `cell 1F-delete-old delete any first inode:first`
- [ ] `cell 1F-diverge diverge any first shared`  (break inode sharing, identical content -> expect clean)

For each row confirm the Q1.1/Q1.2 answers: recovery without full reset? which indexes survived? state of the index whose tx was corrupted?

## Scenario 2 - single-index commit failure

- [x] Covered by the committed tests (see Post-fix retest above): `RavenDB_27156.cs`, `RavenDB_27156_e2e.cs`, `RavenDB_27166.cs`.
- [ ] Real disk-full on F: (below).

### Real disk-full (external F:, NTFS, 16 GB)

Pre-fix result (2026-07-23): **graceful, no crash.** Ballooned F: to ~40 MB free, RESET all 6 indexes to force rebuilds -> `DiskFullException` (Errno 112) on index scratch-buffer growth + on the DOCUMENTS tx merger opening a new journal -> graceful catastrophic-failure DB unload, server stayed alive. After freeing space + restart: DB loaded, all indexes rebuilt to Normal with full entries, documents intact. Did NOT reproduce F-3 (a gradual fill fails at an earlier allocation than the shared-journal write).

- [ ] `set RAVEN_24520_BASE=F:\rdb24520` `set RAVEN_24520_SAMPLE=500` then `... -- seed 2`  (small golden ON F:, so hard links form on F:)
- [ ] `... -- restore-work`
- [ ] `... -- diskfull F:\rdb24520\work 40`
- [ ] Observe: server crash (would mean F-3 regressed) vs graceful DB unload; `HandleDiskFullErrors` retry (x10, FlushAndSync); which env errors first.
- [ ] `... -- verify F:\rdb24520\work` after the balloon is freed - confirm recovery + integrity.
- [ ] Remember to `Remove-Item Env:\RAVEN_24520_BASE, Env:\RAVEN_24520_SAMPLE` afterwards.

## Scenario 3 - extra nasty

- [x] Hard-kill mid-merge (fresh unsynced index txs across all branches) - covered by the golden seed itself (it hard-kills while journals are fresh); baseline `verify` = clean recovery.
- [x] Delete the active shared-journal link (root's link) - clean; inode survives via branch links (cell 1F-delete-active).
- [x] Break inode sharing with identical content - clean (cell 1F-diverge).
- [x] Dangerous recovery flag - `$env:RAVEN_24520_EXTRA_ARGS='--Storage.Dangerous.IgnoreInvalidJournalErrors=true'` then rerun a payload cell -> skips the invalid journal, fewer indexes reset, but **silent partial data loss** (F-4).
- [ ] `--Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions=true` variant (only meaningful on already-synced corrupted txs).
- [ ] Encrypted DB variant (corrupt -> decrypt-failure path in `JournalReader.TryValidateTransaction`). Needs a server built with `-p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP` + encrypted DB setup (cluster bootstrap -> PutSecretKey -> CreateDatabase Encrypted=true). Expected: same cross-index cascade, surfaced as a decrypt failure. NOT YET RUN.

## Notes / observations

- The AV that dominated the pre-fix campaign is gone; keep the AV loop in the retest section as the regression gate.
- If a cell ever reports the server process dying (rather than an index erroring), treat it as an F-3-class regression and capture a dump (`DOTNET_DbgEnableMiniDump=1`, `DOTNET_DbgMiniDumpType=2`).
