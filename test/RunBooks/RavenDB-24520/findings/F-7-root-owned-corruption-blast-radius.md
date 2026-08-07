# F-7 - Corruption of a root-owned transaction still fails the whole database load

**Severity:** medium. This is the blast radius that survived RavenDB-27278. Documents are safe and indexes are rebuildable, but the failure is database-wide instead of one index, and it lands on the region of the journal most likely to be damaged.
**Status:** open - not filed yet. Found 2026-08-07 on the rebased branch (27278 in place).
**Evidence:** observed (test) + derived (server init path).
**Shared context:** see [README.md](README.md). This is the residual part of [F-1](F-1-no-isolation.md) that 27278 did **not** close.

## Claim

RavenDB-27278 confined a corrupted transaction's damage to its owning environment - for *branches*. The shared-journal **root** (`Indexes/@SharedJournals`) is not just another sibling: if the root cannot open, index initialization never starts and the whole database fails to load.

The fix's own e2e test acknowledges this. `RavenDB_27278_e2e` deliberately selects a victim transaction with no later root transaction, commenting: "the root failing to open fails the WHOLE database load, not just indexes". So the hole is known - it just is not written down anywhere outside a test comment.

## Mechanism

**Voron side.** The root env replays the shared journal like any other participant and applies the same rules. Corrupt a root-owned transaction that has later work after it and the root's recovery fails.

**Server side (verified, not inferred from the test comment).** `IndexStore.InitializeAsync` ([IndexStore.cs:850-874](../../../../src/Raven.Server/Documents/Indexes/IndexStore.cs)):

```csharp
return InitializeSharedJournalsAsync()
    .ContinueWith(t =>
    {
        if (t.IsCompletedSuccessfully is false)
            return t;                       // <-- OpenIndexesFromRecord is never reached
        ...
        OpenIndexesFromRecord(record, raftIndex, addToInitLog);
        return Task.CompletedTask;
    }).Unwrap();
```

`InitializeSharedJournalsAsync` constructs `SharedIndexJournals`, which opens the root `StorageEnvironment`. If that throws, the faulted task is propagated out of `InitializeAsync` and indexes are never opened at all. `Core.ThrowIfAnyIndexCannotBeOpened=false` does not help, because the code never gets as far as opening an index.

## Observed

Test: `FastTests.Voron.SharedJournal.RavenDB_24520.CorruptedRootTransactionStillFailsTheWholeSharedJournalRoot`.

Transaction ownership in the fixture's shared journal:

| Owner | Count | Offsets |
|---|---|---|
| **root** | 2 | **0, 4096** |
| link record | 2 | 12288, 20480 |
| branch A | 3 | 8192, 24576, 32768 |
| branch B | 2 | 16384, 28672 |

Corrupting the root's first transaction (offset 0) fails the root with:

```
InvalidJournalException: Transaction 2 (which has a valid hash) is the first transaction of this environment
found in journal ...\0000000000000000000.journal, but the recovery has resumed past an invalid transaction at
position 0 and this environment has nothing synced - the invalid region could have contained its earlier
transactions.
```

That is the chain-start guard 27278 added, and the message is genuinely good - it names the position and explains the reasoning. The problem is not the diagnostics, it is the scope of the consequence.

## Why this matters more than the transaction count suggests

The root owns few transactions (2 of 9 here; in production the root env "has almost no transactions of its own"). That sounds reassuring, but:

- **The root's transactions sit at the very start of the journal file** - offsets 0 and 4096 here. A torn write, a partially-allocated file, or a damaged leading region hits exactly that area.
- **The consequence is asymmetric.** A branch-owned corruption now costs one index rebuild. A root-owned corruption at offset 0 costs the entire database's availability until an operator intervenes.
- The root is also the one env whose recovery every branch depends on, so there is no "reset just this" recovery path.

## Operator recovery paths (untested)

- `Indexing.DisableSharedJournals=true` should let the database load without the shared root, at the cost of turning the feature off (restart required).
- `Storage.Dangerous.IgnoreInvalidJournalErrors=true` skips the invalid journal - see [F-4](F-4-dangerous-flag-partial-loss.md) for what that costs.

Neither has been verified for this specific case. Worth doing before filing, since "what do I actually do about it" is the first question this raises.

## Fix direction (needs a decision)

The root's data is trivially reconstructible - it holds journal bookkeeping, not index content. Options, roughly in increasing order of effort:

1. **Fail soft:** treat a root that cannot open as "shared journals unavailable" and fall back to per-index journals for this load, marking indexes for rebuild. Keeps the database up.
2. **Rebuild the root** rather than replay it, when its recovery fails and no branch depends on the lost root transactions.
3. Leave the behaviour, document it, and make the error state plainly that this is the shared-journal root and that `DisableSharedJournals` is the escape hatch.

Option 1 matches how indexes are treated elsewhere (rebuildable, so prefer availability), and is the one I would argue for. Option 3 is the cheap floor.
