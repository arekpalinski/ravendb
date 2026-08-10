# F-3 - IO error / ENOSPC at the shared-journal write crashes the process (ACCESS_VIOLATION)

**Severity:** serious (whole-process crash / memory corruption, affects all databases on the node)
**Status: FIXED** - root-caused and fixed under [RavenDB-27156](http://issues.ravendb.net/issue/RavenDB-27156) + [RavenDB-27166](http://issues.ravendb.net/issue/RavenDB-27166).
**Evidence:** observed (crash + dumps), root cause confirmed by the fix.
**Shared context:** see [README.md](README.md).
**Cross-platform:** re-validated on Linux 2026-08-10. The AV loop ran **12/12 clean, 0 crashes** (checking both exit codes >128 and `SIGSEGV`/fatal markers, since `dotnet test` can report a child crash as a plain exit 1), and no server process died in any of the 16 corruption cells or any of the 9 disk-full runs. A **real disk-full also exercised the poisoning path on Linux**, reproducing the Windows chain: with 4 MB journals and 250 MB headroom the failure landed inside the root's merged write (`WriteSharedJournals` / `NextFile` / `CreateJournalWriter`, `DiskFullException` opening a new journal file), `SharedJournalState.SetException` + `MarkCatastrophicFailure` fired, and the unload named a **branch** env (`Indexes/Questions_Search`) - the proof that participants beyond the root were poisoned. Server stayed alive; recovery was exact (docs 173,921, `Users/Search` back to 130,677). Reaching that path needed the journal size lowered: at the default 16 MB the rebuild always failed at index data-pager growth first, 6 runs out of 6. Details and the recipe are in [../20-LINUX-runbook.md](../20-LINUX-runbook.md).

## Claim (as originally found)

When a journal write fails on the per-database shared index-journals root environment (`Indexes/@SharedJournals`), the server process intermittently died with an ACCESS_VIOLATION (exit `0xC0000005`) instead of failing gracefully. Because the shared root serves every index in the database, a single failed index-journal write could take down the whole server process (all databases on the node).

## Evidence (observed, pre-fix)

- ~40-60% of runs of the Scenario-2 repro crashed with exit code `0xC0000005`. Reproduced across 3 independent 5-8x run batches.
- **Not the test seam:** reproduced with `NumberOf4KbsToWrite = 0`, where the seam only `throw`s a managed `IOException` at the very top of `JournalWriter.Write` before any native call - identical to what a real ENOSPC does via `PalHelper.ThrowLastError`. So the crash was in the product's reaction to a failed shared-journal write.
- **Timing:** the crash happened during failure handling - after the write threw, before/around the catastrophic-failure DB unload - not during recovery/restart. Established with a file-based phase trace.
- Faulting sites differed across crashes (heap-corruption signature, not one clean UAF site):
  - Dump A: `CatastrophicFailureHandler.Execute` unload task concurrent with the `SharedIndexJournals.WriteSharedJournals` merger thread.
  - Dump B: the index's **own indexing thread** faulting on a data-page read of its own env - `Voron.Page.get_PageNumber` <- `LowLevelTransaction.GetPage` <- `Tree.GetReadOnlyPage` <- `CollectionOfBloomFilters.BloomFilter.GetPartitionByNumber` <- `MapIndexBase.UpdateIndexEntriesLucene` <- `MapItems.Execute` <- `Index.ExecuteIndexing`. No shared-journal frames.

## Root cause (confirmed)

A failed shared-journal write **corrupts the branch environments' in-memory scratch state**: the rolled-back branch transaction resurrects scratch entries that were already freed (the tx had applied a piggybacked journal flush-state update), so later readers/writers on that env resolve pages to wrong scratch positions - producing wrong-page reads and, once a retrying indexing write tx overwrites a resurrected position, an ACCESS_VIOLATION.

Pre-fix, only the **root** env was poisoned on a failed write (in `JournalFile.Write`); the **branches** merely got their commit `TaskCompletionSource` faulted and **kept running on corrupted state** - which is why the AV surfaced in the indexing thread with no shared-journal frames.

Per Oren's directive on the issue: a commit failure is non-recoverable (we do not know what made it into the shared journal), so **all** environments taking part in the failed commit must be forced to restart.

Investigation note: a dispose-ordering hypothesis (merger thread racing the parallel branch-env disposal in `IndexStore.Dispose`) was tested and **refuted** - reordering the disposal left the crash rate unchanged (5 AV / 14 runs, ~36%, vs the ~40-60% baseline). That correctly redirected the search to state corruption in the failure path rather than teardown ordering.

## The fix

`RavenDB-27156` - **poison every participant of a failed shared-journal write.** In `SharedJournalState.SetException`, each participating environment is marked catastrophically failed (`SetCatastrophicFailure`) before its TCS is faulted, covering both the merged batch and queued-not-yet-merged commits. All envs of a database share one `CatastrophicFailureNotification`, so N poisoned envs still produce exactly one DB unload. The designed-recoverable hard-link fallback path is unaffected (it delivers `HardLinkLimitExceededException` directly, bypassing `SetException`), and clean shutdown uses the separate `SetCancel`.

`RavenDB-27166` - **poison on rollback** when a transaction that applied a piggybacked journal flush-state update rolls back (the same corruption reachable without shared journals), narrowed to the case where it would really restore freed scratch pages.

Plus two adjacent recovery fixes found along the way: undersized data pager when recovering a journal with an incomplete final transaction, and publishing the data-pager state grown by a journal skipped via `IgnoreInvalidJournalErrors`.

Commits on this branch: `5e4f97e32d5`, `c29f2827e5e`, `bb250bd7d1b`, `3de087cd3a0`, `dd66e0ac6c8` (27156); `a339db90572`, `6bbc9339f6d`, `946cbe9ea3a` (27166).

## Scope note - real gradual disk-full did NOT trigger F-3 (observed)

On external F: (16 GB NTFS): ballooned to ~40 MB free, RESET all 6 indexes to force rebuilds. The genuine ENOSPC surfaced as `DiskFullException` (Errno 112) on index scratch-buffer growth and on the DOCUMENTS transaction merger opening a new journal, which triggered a graceful catastrophic-failure DB unload - **no process crash, server stayed alive.** After freeing space + restart: DB loaded, all indexes rebuilt to Normal with full entries, documents intact.

So a gradual fill fails at an *earlier* allocation (scratch / documents journal) and degrades gracefully. F-3 needed the failure to land specifically on the shared-journal write call - a transient IO error mid-write, or ENOSPC hit exactly there - which the injection seam models. This narrowed the real-world trigger but did not invalidate it.

## Repro / regression

Committed regression tests (run these, they replace the campaign's throwaway repro):
```bash
dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~RavenDB_27156|FullyQualifiedName~RavenDB_27166"
```
The AV repro specifically (loop it; pre-fix ~40-60% crashed, post-fix expected 0):
```bash
dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~RavenDB_27156_e2e.TornJournalWrite_OnSharedRoot"
```
Crash-dump capture (if it ever returns): `DOTNET_DbgEnableMiniDump=1`, `DOTNET_DbgMiniDumpType=2`, then `dotnet-dump analyze <dmp>` + `clrstack -all`. For a native heap UAF, PageHeap / Application Verifier (`gflags /p /enable Raven.Server.exe /full`) pins the exact invalid free/use.

## Post-fix verification (this branch, Windows, 2026-08-04/05)

1. **Fix tests**: `RavenDB_27156` + `RavenDB_27156_e2e` + `RavenDB_27166` -> 8/8 green.
2. **Injection AV loop**: the e2e torn-write repro run 14x -> **14/14 PASS, 0 ACCESS_VIOLATION** (pre-fix baseline ~40-60% crashed).
3. **Corruption matrix (15 cells)**: DB loaded in every cell, no process crash anywhere.
4. **Real ENOSPC at the shared-journal write (the strongest validation, 2026-08-05)**: with a ~130k-doc golden on external F: and only 100 MB free, forcing all 6 index rebuilds produced a genuine `DiskFullException` inside `WriteAheadJournal` ("The disk is full! ... Failed to increase file ...").
   - **The fix fired on all participants**: both the root env (`so.Indexes.@SharedJournals`) and a *branch* index env (`Users/Search`) entered `CatastrophicFailure` state. Pre-fix, only the root was poisoned while branches kept running on corrupted scratch state - the exact condition that produced the AV.
   - Single `CatastrophicFailureHandler` DB unload (`DatabaseDisabledException` to clients), **server process alive, no ACCESS_VIOLATION**.
   - After freeing the space: DB loaded, doc count exact (152,534 = 136,534 baseline + 16,000 primed), all 6 indexes Normal with full entries, 0 index errors - **no data loss**.

Note this supersedes the pre-fix scope note above: a real gradual disk-full *can* reach the shared-journal write, it just needs a workload large enough to force new allocation. On a small dataset Voron's preallocated 16 MB journal and scratch buffers absorb everything, so runs at 40 MB / 8 MB / even 1 MB free produced no failure at all (inconclusive, not passes). See the methodology warning in `../10-WINDOWS-runbook.md`.

See `../10-WINDOWS-runbook.md` "Post-fix retest" + "Real disk-full" for the full recorded results and re-run recipes.
