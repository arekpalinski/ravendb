# RavenDB-24520 Shared Journals - Failure/Corruption/Recovery Testing Reference

Reference for the [RavenDB-24520](http://issues.ravendb.net/issue/RavenDB-24520) test campaign. Read this first, then follow the OS-specific runbook ([10-WINDOWS-runbook.md](10-WINDOWS-runbook.md), [20-LINUX-runbook.md](20-LINUX-runbook.md)). Results live in [FINDINGS.md](FINDINGS.md) -> [findings/](findings/).

## What is being tested

Voron v8 Shared Journals: one physical journal directory per database at `Databases/<db>/Indexes/@SharedJournals/Journals`. Each index storage environment ("branch") hard-links those journal files into its own `Journals/` directory. All branch commits are merged and written by the single root env (`SharedIndexJournals`). Recovery replays one physical file into N environments, filtering transactions by the per-env `JournalId` Guid stamped in each `TransactionHeader` (offset 136).

Three attack surfaces:
1. Journal file corruption at rest (Scenario 1).
2. Transactional commit failure of a single index, incl. real disk-full (Scenario 2).
3. Extra real-world faults (kill-mid-merge, link-topology games, stale journals, lenient-recovery flags, encryption).

## Key code (v8.0)

- Root env owner + merger thread: `src/Raven.Server/Documents/Indexes/SharedIndexJournals.cs`
- Merged branch state (+ the RavenDB-27156 poisoning fix): `src/Voron/Impl/Journal/SharedJournalState.cs`
- Branch attach: `IndexStore.RegisterSharedJournals` -> `SharedIndexJournals.Register` sets `branchOptions.RootJournal`.
- DB load chains on shared journals: `IndexStore.InitializeAsync` -> `InitializeSharedJournalsAsync`. A corrupt `@SharedJournals` root that fails to open fails the whole DB load.
- Recovery + per-env `JournalId` filter + corruption detection: `src/Voron/Impl/Journal/JournalReader.cs` (`TryReadAndValidateHeader`, `ValidatePagesHash`, `VerifyTransactionSequence`, `VerifyNoUnexpectedValidTransactionsAfter`, `ProcessLinkedJournalsRecord`).
- Merge + branch commit path: `src/Voron/Impl/Journal/WriteAheadJournal.cs` (`WriteToJournal`, `SubmitBranchJournalEntry`, `WriteBuffersToJournal`, `FlushMergedJournalEntries`).
- Journal write: `src/Voron/Impl/Journal/JournalWriter.cs` `Write` (+ the partial-write test seam).
- Catastrophic failure -> DB unload after `MaxDatabaseUnloads=3` in 15 min: `src/Raven.Server/Documents/CatastrophicFailureHandler.cs`.
- Index-side error handling: `Index.cs` `HandleIndexCorruption` / `HandleDiskFullErrors` (retry x10 w/ FlushAndSync) / `HandleWriteErrors`.

## Test seam (committed with RavenDB-27156)

`StorageEnvironmentOptions.TestingStuff.SimulatePartialJournalWriteFailure` (`Func<long, PartialJournalWriteFailure>`) - honored in `JournalWriter.Write`. Writes only the first N 4KB blocks of a batch then throws the supplied exception, simulating a torn/partial journal write (real disk-full mid-write, IO error). Existing seams throw *before* the write, leaving nothing on disk; this one models the real thing. `NumberOf4KbsToWrite = 0` makes it a pure pre-write throw, exactly what a real ENOSPC does via `PalHelper.ThrowLastError` - useful to prove a finding isn't an artifact of the seam.

## Harness (test/Tryouts)

Files: `test/Tryouts/RavenDB_24520/{Harness,JournalTools,Scenarios}.cs`, driven by `test/Tryouts/Program.cs`. Runs an **external** `Raven.Server` child process so it can hard-kill it and corrupt files at rest.

Build: `dotnet build test/Tryouts -c Release`
Run:   `dotnet run --project test/Tryouts -c Release --no-build -- <command>`

Environment overrides:
- `RAVEN_24520_BASE`    base dir for all state (default `D:\temp\24520`; on Linux use e.g. `/tmp/24520`).
- `RAVEN_24520_DUMPS`   source dumps dir (default `D:\workspace\stackoverflow-data-small`).
- `RAVEN_24520_INDEXES` SO index definitions dump (default `D:\workspace\stackoverflow-data\SO-indexes.ravendbdump`).
- `RAVEN_24520_SAMPLE`  keep 1/N docs by numeric id (default 50) - keeps the DB small enough to copy per cell.
- `RAVEN_24520_EXTRA_ARGS` extra server args, space-separated (e.g. dangerous recovery flags).

Layout under BASE: `golden/` (read-only reference DB, hard-killed with fresh journals), `work/` (disposable copy the cells corrupt), `staging-dumps/`, `logs/`, `baseline.json`, `findings-scenario1.md`.

### Commands

- `seed [numPostsDumps]` - fresh golden DB: import dumps (sampled, see dataset note), import SO indexes, wait for non-stale, write burst into Questions+Users, hard-kill mid-fresh-journal, snapshot inode topology into `link-manifest.json`.
- `map [dir]` - print environments (JournalId, sync state) + per-inode transaction maps (owner, tx id, hash validity, link-records). Use this to aim corruption.
- `status [dir]` - journal files + hard-link counts.
- `restore-work` - reset `work/` from `golden/`, re-creating the hard-link topology (a plain copy breaks links; the manifest restores them).
- `verify [dir]` - start a server on the dir, report DB-load success, per-index state/entries, index errors, doc count vs baseline, scan logs for ERROR/FATAL. Kills the server after.
- `cell <name> <op> <ownerFilter> <which> [fileSelector]` - restore-work -> apply ONE corruption -> verify -> append a findings row. See the Windows runbook for the matrix.
- `diskfull <dir> <leaveMB>` - real disk-full drive: prime+settle indexing, balloon the volume, then RESET all indexes to force heavy shared-journal writes with no free space.
- `server [dir]` - start a server and leave it running until ENTER (manual poking).

Journal-file sizing: the harness starts servers with `--Storage.MaxJournalFileSizeInMb=16` so you get many small journals - faster per-cell restore and a richer multi-file corruption topology.

### Dataset note (important)

The StackOverflow dumps are tagged `BuildVersion=40000` (classified V4) but carry the V3-era `Raven-Entity-Name` metadata key. The server only translates that key to `@collection` for V3 dumps, so a plain `import-dir` drops the collection and every document lands in `@empty` -> the SO indexes (`docs.Questions`, `docs.Users`) match nothing and stay empty. The harness works around this by importing client-side per file with a transform script that also samples:

```
var m = this['@metadata'];
if (m['Raven-Entity-Name']) { m['@collection'] = m['Raven-Entity-Name']; }
var id = m['@id']; var n = parseInt(id.substring(id.indexOf('/')+1));
if (isNaN(n) === false && (n % <SAMPLE>) !== 0) throw 'skip';
```

This yields collections `Questions` (from posts-*.dump, IDs `questions/*`, embedded `Answers`) and `Users` (from users.dump). A smuggler-compat quirk of these specific old dumps, not a shared-journals issue - noted so the Linux re-run behaves identically. **`import-dir` alone will NOT populate the indexes.**

## Integrity check after every recovery

`verify` asserts: DB loads; doc count >= baseline; every index `state != Error` and `IsInvalidIndex == false`; no index errors; no missing indexes; logs free of unexpected ERROR/FATAL. A "clean recovery" row means all of that held.
