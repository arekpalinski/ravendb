# F-9 - A missing journal file in the shared root's directory fails the whole database load

**Severity:** medium-low. Total loss of database availability, but there is an escape hatch that **works**, and no data is lost. The complaint is that the error does not mention the escape hatch.
**Status:** open - not filed. Found 2026-08-07 during the post-27278 matrix re-baseline (cell `1F-delete-old`).
**Evidence:** observed (server level, 3 runs: no flag, with flag, message capture).
**Shared context:** see [README.md](README.md). This is the F-7 mechanism reached by a route that actually occurs - see [F-7](F-7-root-owned-corruption-blast-radius.md), whose own claim was refuted.

## Claim

Deleting a single journal file from `Indexes/@SharedJournals/Journals/` fails the **entire database load**, not just indexing. The dangerous flag recovers it completely.

## Observed

Cell `1F-delete-old`: delete the oldest shared journal (`0000000000000000001.journal`, hard-link count 2 - the root plus one branch).

| Run | Outcome |
|---|---|
| default | **`DatabaseLoadFailureException: Failed to start database so`** - database will not load at all |
| `--Storage.Dangerous.IgnoreInvalidJournalErrors=true` | **full recovery**: docs 136,534 (exact), all 6 indexes `State=Normal` with baseline entry counts, `verify => OK` |

Underlying error and path:

```
Voron.Exceptions.InvalidJournalException: No such journal
  '...\Indexes\@SharedJournals\Journals\0000000000000000001.journal'.
  Journal details: LastSyncedJournal - 1, LastSyncedTransactionId - 2, Flags - None
    at StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.GetJournalFileInfo
    at ...OpenJournalPager
    at WriteAheadJournal.RecoverDatabase
    at StorageEnvironment.LoadExistingDatabase
    at StorageEnvironment..ctor
    at SharedIndexJournals..ctor            <- fails IndexStore.InitializeAsync -> whole database
```

## Why this route works where F-7's did not

F-7 tried to reach the same whole-database failure by *corrupting* a root transaction, and that is unreachable: the root's only transaction is its initializing one, it is already synced, and recovery skips already-synced transactions **without validating them**.

A **missing file** is different - it is checked unconditionally in `GetJournalFileInfo`, regardless of sync state. Note `LastSyncedJournal - 1`: journal 001 *is* the last synced journal and its transactions are all synced, yet its mere absence is fatal because recovery starts from `LastSyncedJournal`.

So sync state protects against corrupted bytes but not against a missing file.

## Realistic triggers

- **Manual cleanup.** Operators do delete journal files, and [F-7](F-7-root-owned-corruption-blast-radius.md) notes that the natural remedy for a broken shared root is "delete `@SharedJournals` and let indexes rebuild". Doing that *partially* - removing one file rather than the whole directory - turns an index problem into a total outage.
- **Backup / restore or file sync that does not preserve hard links.** The root's journals are hard-linked into every branch directory, so there are many filesystem locations where tooling can break the set.
- A dropped link that the `LinkedJournalsRecord` repair cannot fix: that record re-creates *branch* links **from** the root's copy, so if the root's copy is the one missing there is nothing to repair from. See [F-6](F-6-linkrecord-bypass-diagnostics.md).

## An asymmetry worth knowing

| Deleted | Result |
|---|---|
| the **active** shared journal, via the root's link (`1F-delete-active`) | **clean** - the inode survives via the branch hard links |
| an **old** shared journal from the root's directory (`1F-delete-old`) | **whole database fails to load** |
| a **branch's** link (F-6 setup) | that branch fails loudly; other indexes and the database are fine |

Deleting the root's link to the *active* journal is harmless while deleting its link to an *old* one is fatal. That is not intuitive, and an operator cleaning up "old" journals would pick exactly the fatal one.

## The actual complaint

The escape hatch works, so this is recoverable. But the error the operator gets is:

> `InvalidJournalException: No such journal '...0000000000000000001.journal'. Journal details: LastSyncedJournal - 1, LastSyncedTransactionId - 2, Flags - None`

and nothing else. **No remedy is offered.** Compare the branch-level message for the same class of problem, which is genuinely good:

> Failed to open a storage at `...\Indexes\Questions_Tags_ByMonths` ... The recommended approach is to **reset the index** ... Alternatively you can temporarily start the server in **dangerous mode** so it will ignore invalid journals on startup.

An operator hitting F-9 sees a dead database and a bare "No such journal", with no hint that `Storage.Dangerous.IgnoreInvalidJournalErrors=true` recovers it fully. That is the fix worth making: carry the same remedy text onto the missing-journal path, and say when the failing environment is the shared-journal root.

## Repro

```bash
# fails the whole database
dotnet run --project test/Tryouts -c Release --no-build -- cell 1F-delete-old delete any first inode:first

# recovers completely
$env:RAVEN_24520_EXTRA_ARGS='--Storage.Dangerous.IgnoreInvalidJournalErrors=true'
dotnet run --project test/Tryouts -c Release --no-build -- cell 1F-delete-old-ignoreflag delete any first inode:first
$env:RAVEN_24520_EXTRA_ARGS = $null
```
