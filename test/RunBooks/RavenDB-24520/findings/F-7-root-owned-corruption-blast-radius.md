# F-7 - REFUTED: corrupting the shared-journal root does not fail a real database

**Status: REFUTED at server level, 2026-08-07.** Do not file. Kept because commits reference F-7 and because the reason it failed is the most useful thing this round produced (see "Why it failed" - it governs the reachability of a whole class of corruption tests).
**Evidence:** observed (Voron 4-cell matrix + server-level e2e).
**Shared context:** see [README.md](README.md).

## What was claimed, in two successive wrong versions

**v1:** "corruption of a root-owned transaction fails the whole database load." Refuted by the Voron matrix - only the environment-**initializing** transaction is fatal; an ordinary root transaction truncates and the root opens.

**v2:** "damage to the shared journal's first 4KB fails the whole database, because that block carries the root's initializing transaction, and unlike the non-shared design that file is hard-linked by every index." Refuted by the server-level test below.

## Why it failed

Test: `SlowTests.Voron.Issues.RavenDB_24520_e2e.DamagedSharedRoot_IsItReachable_AndDoesDisableSharedJournalsRescueIt`. A normal database, 3 indexes, 500 documents, then the root's transaction at offset 0 is corrupted and the database reloaded.

```
root journal info: LastSyncedJournal=0, LastSyncedTransactionId=1
0000000000000000000.journal - root: 1 tx(s) at 0
                            - branch: 6 tx(s) at 4096, 12288, 36864, 40960, 45056, 49152
                            - linkRecord: 2 tx(s) at 8192, 53248
corrupting root tx 1 at offset 0
database LOADED with the shared root damaged
  Idx/A: State=Normal, Entries=500 | Idx/B: State=Normal, Entries=500 | Idx/C: State=Normal, Entries=500
```

Three reasons it is unreachable:

1. **The root owns exactly one transaction** - its initializing one - and in a real database it is **already synced** (`LastSyncedTransactionId=1`). `IsAlreadySyncTransaction` therefore skips it during recovery **without validating it at all**, so its bytes can be arbitrary garbage and nothing notices.
2. The chain-start guard added by RavenDB-27278 requires `LastSyncedTransactionId == -1`, so it cannot fire on a root that has synced.
3. There are no other root transactions to attack.

The question of whether `Indexing.DisableSharedJournals=true` rescues a damaged root is therefore **moot** - there is nothing to rescue. It was never exercised.

## The methodology lesson (the actually useful part)

The Voron-level fixture reproduced F-7 only because it sets `ManualFlushing = true; ManualSyncing = true` from environment creation, so **nothing is ever synced** and every transaction in the file gets fully validated on replay.

A real environment is not in that state. It syncs promptly, and from then on recovery **skips already-synced transactions without validating them**:

```csharp
private bool IsAlreadySyncTransaction(long transactionId)
{
    return _journalInfo.LastSyncedTransactionId != -1 && transactionId <= _journalInfo.LastSyncedTransactionId;
}
```

So sync state, not corruption kind or position, is the first-order control on whether a corruption scenario is reachable at all:

| Environment state | Effect on a corrupted transaction |
|---|---|
| nothing synced (`LastSyncedTransactionId == -1`) - the test fixture | every transaction validated, corruption detected, guards active |
| normally synced - production | transactions at or below the synced id skipped **unvalidated**; only later ones are checked |

**Consequence for this campaign and any future one:** a corruption finding produced with `ManualSyncing` set must be re-tested at server level before it is believed. The transactions most likely to be picked as victims (early ones, the initializing transaction, anything at the head of the file) are exactly the ones a real database has already synced past. Two of this round's findings died on precisely this.

Note this cuts the other way for older journals too - `Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions` exists because already-synced data failing validation is considered safe to continue past. F-7's victim was in that category all along.

## What is still worth doing (unrelated to the refuted claim)

The generic Voron message surfacing for a shared index-journal root is poor advice even though it is now known to be hard to reach:

> First transaction initializing the structure of Voron database is corrupted. Cannot access internal database metadata. **Create a new database to recover.**

For `Indexes/@SharedJournals` the correct remedy is to delete that directory and let indexes rebuild, not to create a new database. If it is ever cheap to special-case the message when `Options.RootJournal is null` and the environment is the shared-journal root, it is worth doing. Low priority - the path requires an unsynced root, which this test shows does not occur in normal operation.
