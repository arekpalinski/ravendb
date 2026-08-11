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
- `RAVEN_24520_JOURNAL_MB` shared/branch journal size (default 16). Lower it to 4 to make the disk-full scenario reach the root's *merged write* - at 16 MB the post-reset rebuild always fails on index data-pager growth first. See the Linux runbook.
- `RAVEN_24520_LOGS`    log directory (default `<BASE>/logs`). Point it **off** the volume under test for disk-full runs, or the verdict is computed from logs whose own writes are failing.
- `RAVEN_24520_ENCRYPTED` `1` to seed an **encrypted** database (see below).

### Encrypted variant (`RAVEN_24520_ENCRYPTED=1`)

Corruption then surfaces as a decrypt/MAC failure instead of a hash mismatch. Three prerequisites, and the first one bites hard:

1. **Build in this order, and use `--no-incremental`:**
   ```
   dotnet build test/Tryouts -c Release                                  # FIRST
   dotnet build src/Raven.Server -c Release -p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP --no-incremental   # LAST
   ```
   Two independent traps here, and **both produce the same runtime error** as a missing license:
   - Building `test/Tryouts` compiles `src/Raven.Server` as a dependency **without** the property, silently stripping the define. So the flagged build must come last, and must be repeated after any later Tryouts rebuild.
   - Without `--no-incremental`, MSBuild reports `Build succeeded` in seconds **without recompiling** - a changed property does not invalidate its up-to-date check - so the define never reaches the binary at all.

   All three failures surface as `Database so is encrypted and requires 1 node(s) which supports SSL. There are 0 such node(s) available in the cluster.` Diagnose in order: did the flagged build emit a `Raven.Server -> ...dll` line? was anything built after it? only then suspect the license. The gate is `Server.AllowEncryptedDatabasesOverHttp`, read in `ServerStore.cs`.
2. **A license** - encryption is a licensed feature. Write one to a file and pass `RAVEN_24520_EXTRA_ARGS=--License.Path=<file>`.
3. The harness then does the rest during `seed`: `POST /admin/cluster/bootstrap` (to leave passive state, which `PutSecretKey` requires), generates a 256-bit key and installs it via `POST /admin/secrets?name=so&overwrite=true` with the base64 key as the raw body, then creates the database with `Encrypted = true`. The key is also written to `<BASE>/secret.key.base64`.

The secret key lives in the server store **inside `DataDir`**, so it travels with `golden` -> `work` copies and `restore-work` / `verify` / `cell` need no extra setup on the same machine and user account. It is *not* portable across machines or users (on Windows the store's protection is DPAPI, user-scoped), so an encrypted golden cannot be moved between boxes - seed one per box.

Journal parsing is unaffected: only `page + TransactionHeader.SizeOf` onward is encrypted, with the header itself used as AEAD associated data and a MAC in the header, so markers / `TransactionId` / `JournalId` stay readable and cell aiming works exactly as on a plain database. `map` prints `hash=n/a (encrypted)` for such transactions, because `header->Hash` is not a plaintext XXHash64 of a ciphertext payload - do not read that as corruption.

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
