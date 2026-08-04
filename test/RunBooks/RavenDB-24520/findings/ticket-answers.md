# RavenDB-24520 - answers to the ticket questions

The ticket's investigation questions, answered from the campaign. These summarize behavior; the underlying mechanisms live in the finding files.
**Shared context:** see [README.md](README.md).

## Scenario 1 (corruption in the shared journal)

### Q1.1 - Does it recover without a full reset? Can it isolate the damage?
- The DB **always loads** (documents live in a separate environment and stay intact).
- Damage is confined to **index** environments - but **NOT** to a single index. Every index hard-linked to the corrupted physical journal is faulted. See [F-1](F-1-no-isolation.md) for the mechanism (hash validation of foreign-env txs before the owner-filter) and the full corruption-kind matrix. Blast radius = the set of envs linked to that file (active journal = all indexes; old journal = few).
- The dangerous flag can skip the bad journal and salvage more indexes, at the cost of silent partial loss - see [F-4](F-4-dangerous-flag-partial-loss.md).

### Q1.2 - State of the corrupted index + recovery
- First load after corruption: the affected index hits `IndexOpenException` and is opened as a **fake in-memory instance** (entries 0).
- On a subsequent restart it settles into **State=Error** and does **NOT** auto-rebuild (observed: errored for 2 min, no progress).
- **Manual `RESET` (HTTP verb `RESET` on `/databases/<db>/indexes?name=`) fully recovers it**: the index rebuilds from documents to the exact baseline entry counts (observed: Users/Search 122677, Questions/Search 13857, etc.), State=Normal, 0 errors, no document loss.
- The startup recovery-error message is high quality: it names the index, advises resetting it, and mentions `--Storage.Dangerous.IgnoreInvalidJournalErrors=true` for dangerous-mode startup.

## Scenario 2 (single-index commit failure)

### Q2.2 - Does one index's failure affect the others?
- **Yes - no isolation.** All branch commits funnel through the single root merge write (`WriteAheadJournal.FlushMergedJournalEntries`). When that write fails, all indexes sharing the journal go to State=Error simultaneously (observed: 3/3 at t+1s..t+2s). The shared root write is a single point of failure; other indexes cannot commit independently during the failure.
- Post-[F-3](F-3-write-failure-access-violation.md)-fix this is now *explicit and intentional*: a failed shared-journal write poisons every participating environment, because the journal contents are unknown and all participants must restart. So the answer to "can Index_B still commit while Index_A's commit failed" is a deliberate **no**.

### Q2.3 - Aftermath / recovery after restart
- Graceful self-heal: the root env raises catastrophic failure -> `CatastrophicFailureHandler` unloads the DB -> it reloads -> recovery discards the torn/partial tail -> all indexes return to State=Normal and re-index (observed pre-fix, when it didn't crash: back to Normal by t+3s; after explicit restart docCount intact, all indexes Normal with full entries, 0 errors). **No document loss.**
- Pre-fix caveat, now resolved: ~40-60% of the time the process crashed (ACCESS_VIOLATION) during that failure handling instead of self-healing. Fixed under RavenDB-27156 + RavenDB-27166 - see [F-3](F-3-write-failure-access-violation.md).

## Deliverables status

- **Deliverable 1.3** (e2e test that programmatically corrupts a journal and asserts recovery): the harness `cell` command covers the full matrix reproducibly; a committed automated test for the F-1 blast radius is still open pending the F-1 decision.
- **Deliverable 2.3** (e2e test simulating an IO error during a journal write, validating other index envs stay consistent): **done and committed** as `test/SlowTests/Voron/Issues/RavenDB_27156_e2e.cs` + `RavenDB_27156.cs` + `RavenDB_27166.cs`, on the back of the new `SimulatePartialJournalWriteFailure` seam.
- Runbooks: `../00-REFERENCE.md`, `../10-WINDOWS-runbook.md`, `../20-LINUX-runbook.md`.
