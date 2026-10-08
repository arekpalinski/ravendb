# R1 - RavenDB-27651: shared journals, corruption and recovery

Retest of RavenDB-24520 with its own harness. The scenarios, cells and recipes are in
test/RunBooks/RavenDB-24520 (00-REFERENCE, 10-WINDOWS-runbook, 20-LINUX-runbook, findings F-1 to F-9). This runbook lists
what changed and what to add.

## What changed in the harness

- The journal parser understands the 8.0 format: the incarnation comes from the header record at block 0, the owner is
  `JournalId XOR incarnation`, a hash is valid when `(xxhash ^ Hash) == IncarnationTag`. Without that every entry read as
  foreign and invalid. In an encrypted journal the header record is encrypted too, so the parser takes the incarnation from
  the first entry instead: JournalId XOR the known environment id whose IncarnationTag equals the entry's Hash.
- `map` prints `delta=` per transaction and the header record as `owner=<header-record>`; `status` also lists
  `recyclable-journal.*`, `.tmp` and `*.unrecovered` files; new `journal-stats [dir]`.
- `cell ... any ...` never picks the header record; target it by name with ownerFilter `<header-record>`.
- Log signal for a failed shared-journal write: `SharedJournalState.SetException`, then `LowLevelTransaction.FailDurableCommit`,
  then `SetCatastrophicFailure` (`MarkCatastrophicFailure` is gone).

## Setup

As in the RavenDB-24520 runbooks. Windows: `RAVEN_24520_BASE` defaults to `D:\temp\24520`, the disk-full volume is F:.
Linux: set `RAVEN_24520_BASE` under `~`, disk-full on a loop-mounted ext4 (see README).

1. Re-seed the golden with this build: `Tryouts seed`. The old golden is format 25 (pre-merge) and would test the upgrade path instead.
2. `Tryouts map`: every journal starts with `<header-record>`, owners resolve to index names (no `<unknown ...>`), hashes are valid.

## Matrix (Windows and Linux)

| Row | How |
|---|---|
| Scenario 1 cells | the cell list in 00-REFERENCE, `Tryouts cell <name> <op> <ownerFilter> <which> [fileSelector]` |
| Scenario 2, real disk full | `Tryouts diskfull <dir> <leaveMB>`; Linux: `RAVEN_24520_JOURNAL_MB=4`, leaveMB 250 |
| F-3 torn write | `dotnet test test/SlowTests/SlowTests.csproj -c Release --filter "FullyQualifiedName~RavenDB_27156_e2e"`, 14 times |
| Encrypted | the encrypted variant from the runbooks, hash and payload cells |
| Header record, current journal | `Tryouts cell 1H-header-current zero-block "<header-record>" first shared` |
| Header record, older journal | `Tryouts cell 1H-header-older zero-block "<header-record>" first inode:first` |
| Reusable journal in the root pool | `Tryouts restore-work`, then `Tryouts status` on the work dir lists `recyclable-journal.*` under @SharedJournals (if none, skip the row and say so); overwrite 4 KB at offset 8192 of one of them; `Tryouts server`, reset one index in Studio so the journals roll, stop the server (ENTER), `Tryouts verify` |

## Expected results

- `verify` passes for every old cell with the verdict recorded in the RavenDB-24520 runbooks (post-27278 baseline). A changed
  verdict needs a commit that explains it, or a bug.
- Known changes to expect:
  - encrypted hash flip on an own entry: still a no-op. The tag check before decrypting (JournalReader.cs:566) is skipped
    for the environment's own entries (MayBeOwnTransaction), and Hash is outside the AEAD's associated data, so the entry
    decrypts and is accepted;
  - header record of the current journal zeroed: that journal's transactions are skipped as foreign, with no error
    (by design, RavenDB_27397.Corrupted_header_record_in_the_last_journal_loses_only_that_journal). The database loads and the
    affected indexes catch up from the documents; documents are intact;
  - header record of an older journal zeroed: recovery fails loudly for the environments that still need it
    (RavenDB_27397.Corrupted_header_record_in_a_middle_journal_fails_recovery_loudly), never a silent loss.
- Disk full: one database unload, no process crash, exact recovery (scenario 2 recipe). Where the ENOSPC lands is a race:
  on Windows 16 MB journals hit only index data-file growth in 4 of 4 runs (retried, no unload, 0 FATAL), 4 MB journals
  (`RAVEN_24520_JOURNAL_MB=4`) reached the root's merged write on the first try. A run with 0 FATAL did not test the
  shared-journal path; rerun it.
- F-3: 14 of 14 pass, no ACCESS_VIOLATION.
- A corrupted reusable journal in the pool is harmless: reuse writes a new header record first and its old bytes read as
  another incarnation.
