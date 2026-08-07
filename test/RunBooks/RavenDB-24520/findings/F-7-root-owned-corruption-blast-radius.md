# F-7 - When the shared-journal root is unrecoverable, the whole database fails to load and the remedy given is wrong

**Severity:** low-medium. Narrow trigger (damage to the first 4KB of the shared journal), but the consequence is total loss of database availability and the operator is told to do something drastic and unnecessary.
**Status:** open, **claim corrected 2026-08-07** - see "What I got wrong" below.
**Evidence:** observed (4-cell test matrix) + derived (server init path).
**Shared context:** see [README.md](README.md). This is the part of [F-1](F-1-no-isolation.md) that RavenDB-27278 did not close.

## What I got wrong

The original version of this finding claimed "corruption of a **root-owned transaction** fails the whole database load", generalizing from a single test that corrupted the root's *first* transaction. Testing the full matrix refuted that. Corrupting an ordinary root transaction does not fail the database at all. Only the environment-initializing transaction does, and that behaviour is generic to every Voron environment rather than anything to do with shared journals.

## Observed matrix

Test: `FastTests.Voron.SharedJournal.RavenDB_24520.CorruptedRootTransactionStillFailsTheWholeSharedJournalRoot`. The root owns 2 transactions in the fixture, at offsets 0 and 4096.

| Corrupted root tx | `IgnoreInvalidJournalErrors` | Outcome |
|---|---|---|
| index 0 - the env-**initializing** tx, offset 0 | false | fails: `InvalidJournalException` (the chain-start guard 27278 added) |
| index 0 - same | **true** | fails: `VoronUnrecoverableErrorException` - "First transaction initializing the structure of Voron database is corrupted. Cannot access internal database metadata. **Create a new database to recover.**" |
| index 1 - an ordinary root tx, offset 4096 | false | **root opens.** Truncates at the corruption; that transaction's data (`rootTree/root`) is gone |
| index 1 - same | true | **root opens**, same result |

So the dangerous flag is not an escape hatch for the init-tx case: skipping the journal removes the initialization itself, which is why it fails harder rather than softer.

## What is actually shared-journal-specific

The init-transaction behaviour is generic. What shared journals change is **which failure domain that 4KB belongs to**:

- **Without** shared journals, index X's journal block 0 carries index X's initializing transaction. Damage there kills index X. Every other index is untouched.
- **With** shared journals, the file's first block carries the **root** env's initializing transaction, and that file is hard-linked by every index. Damage there fails the root.

And a root that cannot open fails the entire database load, verified server-side rather than inferred - `IndexStore.InitializeAsync` ([IndexStore.cs:850-874](../../../../src/Raven.Server/Documents/Indexes/IndexStore.cs)):

```csharp
return InitializeSharedJournalsAsync()
    .ContinueWith(t =>
    {
        if (t.IsCompletedSuccessfully is false)
            return t;                       // <-- OpenIndexesFromRecord is never reached
        ...
    }).Unwrap();
```

`Core.ThrowIfAnyIndexCannotBeOpened=false` cannot help, because no index is ever opened.

## The actual complaint

Not the failure itself - an unrecoverable environment should fail. Two things around it:

1. **The blast radius is wrong for what was lost.** The root env holds journal bookkeeping, not index content. Every byte it owns is reconstructible by rebuilding indexes. Yet its loss costs the whole database's availability, including documents-only workloads that never touch an index.
2. **The remedy given is wrong and alarming.** The message says "Create a new database to recover." For a *shared index-journal root* that is terrible advice: the correct remedy is to discard `Indexes/@SharedJournals` and let the indexes rebuild. An operator following the message literally would destroy a database over a rebuildable index artifact. The message is generic Voron text surfacing in a context where it does not apply.

## Fix direction

1. **Fail soft on the root** (preferred). Treat a root that cannot open as "shared journals unavailable for this load": recreate the root env, mark indexes for rebuild, keep the database up. This matches how index data is treated everywhere else - rebuildable, so prefer availability. It also removes the wrong-remedy problem entirely.
2. **At minimum, fix the guidance.** When the failing environment is the shared-journal root, say so and name the real remedy (delete the shared journals directory, indexes will rebuild) instead of "create a new database".

Option 2 is cheap and should happen regardless of whether 1 is taken.

## Still untested

`Indexing.DisableSharedJournals=true` as an escape hatch. Logically the root env is never constructed, so the database should load - but whether the indexes can then open against `Journals/` directories holding hard links to the shared file is not obvious and has not been checked. Worth settling before filing, since it may already be the documented workaround.
