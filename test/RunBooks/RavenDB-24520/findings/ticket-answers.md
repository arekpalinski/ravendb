# RavenDB-24520 - answers to the ticket questions

Answered against the **rebased branch** (27156 / 27166 / 27168 / 27220 / 27278 / 26563), re-measured 2026-08-07. These summarize behaviour; mechanisms live in the finding files.
**Shared context:** see [README.md](README.md).

> Rewritten 2026-08-07. The previous version answered from pre-27278 behaviour and was wrong on Q1.1 and Q1.2 - it reported no per-index isolation and `State=Error`, neither of which holds now.

## Scenario 1 (corruption in the shared journal)

### Q1.1 - Does it recover without a full reset? Can it isolate the damage?

**Yes, damage is now isolated to a single index.** This is the headline change from RavenDB-27278.

- The database loads and **documents are never affected** - they live in a separate environment. The one exception is F-9 below.
- A corrupted transaction faults **exactly one index: the one that owns it**, and only when that index has later transactions of its own after the damage point. Siblings and the root are untouched.
- **Corrupting an environment's last transaction is benign for everyone** - clean tail truncation, zero resets. Verified identical for payload flips, header-marker smash, hash, txid, JournalId, zeroed block and mid-file truncation.
- File-level topology is clean too: deleting the *active* shared journal via the root's link (the inode survives via branch hard links), breaking inode sharing with identical content, truncating the tail, and corrupting the `LinkedJournalsRecord`.

Full 16-cell table in [../10-WINDOWS-runbook.md](../10-WINDOWS-runbook.md) ("POST-27278 RE-BASELINE"). For comparison, pre-fix a single corrupted transaction reset **4-5 of 6** indexes; see [F-1](F-1-no-isolation.md) for the old behaviour and why it changed.

**The one exception - [F-9](F-9-missing-root-journal-fails-database.md):** a *missing* journal file in `Indexes/@SharedJournals/Journals/` fails the **whole database load**, because `GetJournalFileInfo` checks file presence unconditionally regardless of sync state. `Storage.Dangerous.IgnoreInvalidJournalErrors=true` recovers it completely.

The dangerous flag no longer costs data - [F-4](F-4-dangerous-flag-partial-loss.md) is closed; the salvaged index recovers to the exact baseline entry count.

### Q1.2 - State of the corrupted index + recovery

- The faulted index comes up with **`State=Normal`, `Type=Faulty`, `entries=0`**. Note it reports **Normal**, not `Error` - `Type` is the field that identifies a faulty index (see the assertion in `RavenDB_27278_e2e`). **Do not poll `State` alone to detect a faulted index.** The previous answer here said `State=Error`; that is no longer what is observed.
- Documents are intact and every other index keeps its full entry count.
- **Recovery is complete, by either route:**
  - manual `RESET` (HTTP verb `RESET` on `/databases/<db>/indexes?name=`) rebuilds from documents to exact baseline counts;
  - or start once with `Storage.Dangerous.IgnoreInvalidJournalErrors=true`, which skips the journal and lets the index re-index and converge - measured at exactly the uncorrupted baseline (10,744 entries) in cell `3B-ignoreflag-first`.
  Index content is derived from documents, so nothing is unrecoverable.
- **The index-path error message is good** - it names the index and offers both remedies:
  > Failed to open a storage at `...\Indexes\<name>` due to invalid or missing journal files ... The recommended approach is to **reset the index** ... Alternatively you can temporarily start the server in **dangerous mode**.

  The missing-journal path on the root does **not** carry this text - that is the F-9 complaint.
- Not re-measured post-fix: whether a *second* restart moves the faulted index from `Normal/Faulty` into `State=Error`. Pre-fix it did and did not auto-rebuild. Carried over as unverified.

## Scenario 2 (single-index commit failure)

### Q2.2 - Does one index's failure affect the others?

**No isolation, and this is deliberate.** All branch commits funnel through the single root merge write. When that write fails, every participating environment is poisoned, because the journal's contents are unknown and all participants must restart (Oren's directive on RavenDB-27156). So "can Index_B commit while Index_A's commit failed" is an intentional **no**.

Re-validated on a **real** disk-full, 2026-08-07:

```
SharedIndexJournals.WriteSharedJournals            <- merger thread
  WriteToJournal -> WriteBuffersToJournal -> FlushMergedJournalEntries
    NextFile -> CreateJournalWriter -> DiskFullException
  SharedJournalState.SetException -> MarkCatastrophicFailure -> SetCatastrophicFailure
```

The `CatastrophicFailureHandler` unload named a **branch** environment, which is the proof that participants beyond the root were poisoned - pre-fix only the root was, and the branches kept running on corrupted scratch state. That was the source of [F-3](F-3-write-failure-access-violation.md).

Note this contrasts with Scenario 1: corruption *at rest* is now isolated per index, while a *write failure* deliberately takes down all participants.

### Q2.3 - Aftermath / recovery after restart

Graceful self-heal, and no data loss:

- catastrophic failure -> one `CatastrophicFailureHandler` DB unload (clients see `DatabaseDisabledException`) -> reload -> recovery discards the torn tail -> all indexes return to Normal.
- Real-ENOSPC recovery measured exactly: **152,534 documents** (= 136,534 baseline + 16,000 primed), all 6 indexes `State=Normal` with full entry counts, zero index errors.
- **Server process stays alive.** The ~40-60% ACCESS_VIOLATION crash rate seen pre-fix is gone: the torn-write repro ran **14/14 clean, 0 access violations**. Fixed under RavenDB-27156 + RavenDB-27166.
- A retained *historical* index error describing the disk-full stays visible on the affected index afterwards. The index itself is Normal and complete.

## Deliverables status

- **Deliverable 1.3** - e2e test that programmatically corrupts a journal and asserts recovery: **done and committed.**
  - `test/SlowTests/Voron/Issues/RavenDB_27278_e2e.cs` - a corrupted transaction of one index marks only that index faulty; siblings and documents unaffected.
  - `test/FastTests/Voron/SharedJournal/RavenDB_27278.cs` - Voron-level isolation, including a corrupted *size* field failing loudly rather than silently swallowing later transactions.
  - `test/FastTests/Voron/SharedJournal/RavenDB_24520.cs` - campaign characterization tests (link-record bypass, root-tx corruption matrix, stale `Root` graft, encrypted resync allocation bound).
  - `test/SlowTests/Voron/Issues/RavenDB_24520_e2e.cs` - server-level reachability check for the shared root.
- **Deliverable 2.3** - e2e test simulating an IO error during a journal write, validating other index envs stay consistent: **done and committed** as `RavenDB_27156_e2e.cs` + `RavenDB_27156.cs` + `RavenDB_27166.cs`, on the `SimulatePartialJournalWriteFailure` seam in `JournalWriter.Write`.
- Runbooks: [../00-REFERENCE.md](../00-REFERENCE.md), [../10-WINDOWS-runbook.md](../10-WINDOWS-runbook.md), [../20-LINUX-runbook.md](../20-LINUX-runbook.md) - **both executed**. Windows 2026-08-04/07, Linux 2026-08-10.
- **Cross-platform status: no behavioral divergence.** The Linux re-run reproduced all 16 corruption cells identically on a seed with a byte-comparable baseline (130,534 / 136,534 docs, 9 inode groups) but a *different* journal layout, which independently confirms that the retired position rule was layout-dependent victim selection. Post-fix suites 28/28 and 10/10; F-3 AV loop 12/12 with zero crashes; real disk-full graceful with exact recovery, and it reached the 27156 poisoning path (merged write, branch env named) once journal size was lowered to 4 MB. Every issue the Linux pass surfaced was in the **test harness**, not the product - seven of them, listed in the Linux runbook.
- Linux-only addition: **corrupting a journal while the server holds it open** (no mandatory locking) - the write always succeeds, the running server never detects it even across a full in-process reload, and after a hard kill the damage is owner-only, exactly as at rest. **No new finding.**

## Recovery flags - both exercised

| Flag | Effect |
|---|---|
| `Storage.Dangerous.IgnoreInvalidJournalErrors=true` | Rescues [F-9](F-9-missing-root-journal-fails-database.md) completely: a database killed by a missing root journal loads and every index recovers to the exact baseline. Verified on both platforms. |
| `Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions` | **`[DefaultValue(true)]`** - so both 16-cell matrices already ran with it on. Comparing `=false` against the default on a genuinely already-synced corrupted transaction: it changes only the **diagnostics** (a real `Invalid hash signature` recovery error vs an "already synced ... Safely continuing" notice). The database loads and every index recovers to the exact baseline either way, because the transaction's data is already in the data file and 27278's resync finds no gap that matters. Run on Linux 2026-08-10; product behavior, not platform-specific. |

## Still open after this campaign

| | |
|---|---|
| [F-9](F-9-missing-root-journal-fails-database.md) | missing root journal fails the whole database; recoverable via the dangerous flag, but the error offers no remedy. Diagnostics half **filed as RavenDB-27293** (PR open); the other two aspects - whether a missing *already-synced* journal need be fatal at all, and the active-vs-old deletion asymmetry - remain unfiled |
| [F-6](F-6-linkrecord-bypass-diagnostics.md) | diagnostics: a bypassed region is never logged, and one corrupted transaction alerts on every index sharing the journal even when nothing is lost. **Deliberately not tracked as a ticket** (decision 2026-08-10) - recorded here only |
| [F-2](F-2-silent-journalid-loss.md) | `JournalId` corruption on a tail transaction drops it with no error. **Deliberately not tracked as a ticket** (decision 2026-08-10) - recorded here only |
| Encrypted-DB variant | **RUN on Linux 2026-08-11** - no new finding. Encrypted shared journals behave exactly like plain ones: same byte-comparable baseline (136,534 docs, 9 inode groups, active journal linked by all 7), same owner-only isolation, same benign tail truncation. Corruption surfaces as `Could not decrypt transaction N` instead of a hash mismatch. Two things the plain matrix could not show: only the **first 40 bytes** of the transaction header are authenticated (`adlen = SizeOf - NonceOffset` = 40), so `JournalId` at offset 136 is **not** covered and the F-2 silent drop stays silent under encryption; and the `hash` op is inert, because `ValidatePagesHash` is never reached on the encrypted path. One diagnostics difference: a benign tail corruption logs FATAL once per environment (7 entries here), amplifying F-6. Detail in [../20-LINUX-runbook.md](../20-LINUX-runbook.md). Still worth a Windows pass for DPAPI/NTFS specifics. |
