# RavenDB-24520 Findings - index & shared context

One self-contained file per finding so any session can pick up a single issue by reading just that file. Start a focused discussion with: "read `test/RunBooks/RavenDB-24520/findings/<file>` and let's go over it."

## Findings

| ID | Severity | Status | One-liner | File |
|----|----------|--------|-----------|------|
| **F-3** | serious | **FIXED** (RavenDB-27156 + RavenDB-27166) | IO error / ENOSPC at the shared-journal write corrupted branch scratch state -> ACCESS_VIOLATION (~40-60% of runs) | [F-3-write-failure-access-violation.md](F-3-write-failure-access-violation.md) |
| **F-1** | design | open | No per-index isolation: recovery hash-validates foreign-env txs before the owner-filter, so one index's corrupted tx faults every index sharing the journal | [F-1-no-isolation.md](F-1-no-isolation.md) |
| **F-2** | minor | open | `JournalId`-field corruption at the tail = clean load, no error, silent drop of that tx | [F-2-silent-journalid-loss.md](F-2-silent-journalid-loss.md) |
| **F-4** | minor | open (by design?) | `--Storage.Dangerous.IgnoreInvalidJournalErrors=true` salvages more indexes but with silent partial data loss | [F-4-dangerous-flag-partial-loss.md](F-4-dangerous-flag-partial-loss.md) |
| Q&A | - | - | Ticket questions Q1.1/Q1.2/Q2.2/Q2.3 answered (incl. reset-recovery characterization) | [ticket-answers.md](ticket-answers.md) |

Evidence levels used in each file: **observed** (measured on this box), **derived** (from code + observation), **hypothesis** (needs more work).

## Environment (all findings)

- Windows 11, branch v8.0-derived (`RavenDB-24520`, based on the tip of `RavenDB-27166`), .NET 10.
- Golden DB = StackOverflow small, sampled 1/50 during import: ~130k docs, 6 map / map-reduce indexes over the `Questions` and `Users` collections. Shared journals engaged: all 6 index environments hard-linked to the per-database `@SharedJournals` root; the active journal is linked by all 6 branches + the root.
- Real disk-full corroboration used an external F: volume (16 GB NTFS).

## Harness (test/Tryouts/RavenDB_24520/)

`Harness.cs`, `JournalTools.cs` (offline journal parser + corruptor), `Scenarios.cs` (the corruption-cell driver). Driven by `test/Tryouts/Program.cs`. Runs an **external** `Raven.Server` child process so it can hard-kill and corrupt files at rest.

Build: `dotnet build test/Tryouts -c Release`
Run:   `dotnet run --project test/Tryouts -c Release --no-build -- <command>`

Commands: `seed [nPostsDumps]`, `map [dir]`, `status [dir]`, `restore-work`, `verify [dir]`, `cell <name> <op> <ownerFilter> <which> [fileSelector]`, `diskfull <dir> <leaveMB>`, `server [dir]`.

Env overrides: `RAVEN_24520_BASE` (default `D:\temp\24520`), `RAVEN_24520_DUMPS`, `RAVEN_24520_INDEXES`, `RAVEN_24520_SAMPLE` (default 50), `RAVEN_24520_EXTRA_ARGS` (extra server args).

Dataset gotcha: the SO dumps are BuildVersion=40000 (classified V4) but carry the V3-era `Raven-Entity-Name` metadata key, which the server only translates to `@collection` for V3. A plain `import-dir` therefore drops the collection and every doc lands in `@empty`, leaving the SO indexes empty. The harness promotes it via an import transform script; see `../00-REFERENCE.md`.

## Key code map

- Shared root env + merger thread: `src/Raven.Server/Documents/Indexes/SharedIndexJournals.cs`
- Merged branch state + the F-3 fix: `src/Voron/Impl/Journal/SharedJournalState.cs`
- Recovery + per-env `JournalId` filter + corruption detection: `src/Voron/Impl/Journal/JournalReader.cs`
- Journal write (+ the `SimulatePartialJournalWriteFailure` test seam): `src/Voron/Impl/Journal/JournalWriter.cs`
- Merge / branch commit path: `src/Voron/Impl/Journal/WriteAheadJournal.cs`
- Catastrophic failure -> DB unload: `src/Raven.Server/Documents/CatastrophicFailureHandler.cs`
- Transaction header (`JournalId` at offset 136): `src/Voron/Impl/Journal/TransactionHeader.cs`

## Regression tests

Committed with the fixes (these supersede the campaign's throwaway repros):
- `test/SlowTests/Voron/Issues/RavenDB_27156.cs` - poisoning + recovery, torn-tail pager growth.
- `test/SlowTests/Voron/Issues/RavenDB_27156_e2e.cs` - the server-level F-3 repro (`TornJournalWrite_OnSharedRoot_...`).
- `test/SlowTests/Voron/Issues/RavenDB_27166.cs` - piggybacked-flush rollback poisoning.
- Product test seam: `StorageEnvironmentOptions.TestingStuff.SimulatePartialJournalWriteFailure`, honored in `JournalWriter.Write`.

Raw Scenario-1 cell table + per-cell logs (not committed, machine-local): `<RAVEN_24520_BASE>\findings-scenario1.md`.
