# F-8 - Refuted hypotheses from the 2026-08-07 resync review (negative results)

**Status:** closed. Recorded so nobody re-runs these, and so the reasoning errors are visible.
**Evidence:** observed (measurements), plus code paths cited below.
**Shared context:** see [README.md](README.md).

After RavenDB-27278 replaced "stop at the first invalid transaction" with "resync forward to the next fully validating one", I predicted four new hazards. **Three were wrong.** The confirmed ones are [F-5](F-5-journalid-impersonation.md) (serious) and [F-7](F-7-root-owned-corruption-blast-radius.md) (medium); [F-6](F-6-linkrecord-bypass-diagnostics.md) survived only as a diagnostics nit.

## Refuted: "a bypassed region is completely silent"

**Predicted:** `TryFindNextValidTransaction` wraps its whole scan in `DisableOnRecoveryErrorHandler()` + `DisableOnIntegrityErrorOfAlreadySyncedDataHandler()` ([JournalReader.cs:801-802](../../../../src/Voron/Impl/Journal/JournalReader.cs)), so a bypass produces no operator signal at all.

**Actually:** the suppression covers only the forward *scan*. The transaction that first fails validation is rejected in the main loop with the handlers still live, so `OnRecoveryError` fires normally. Measured by collecting handler callbacks in a test: two errors raised. At server level `DocumentDatabase.HandleOnRecoveryError` logs **Fatal** and raises an `AlertRaised` with `NotificationSeverity.Error`.

**My error:** I read the suppression scope and generalized it to the whole bypass without checking where the first failure is detected.

Residual (real but minor): nothing logs the *resume itself*. `_resyncedFromInvalid4KbPosition` / `_resyncedToValid4KbPosition` are tracked but only decorate exception messages that are not thrown when the bypass is judged safe. So the extent of the bypassed region is never reported when recovery succeeds. One Warn line would close it. Folded into [F-6](F-6-linkrecord-bypass-diagnostics.md).

## Refuted: "the encrypted resync scan retains one buffer per probed 4KB boundary"

**Predicted:** for encrypted journals `TryValidateTransaction` allocates a 4KB-aligned buffer and appends it to `_encryptionBuffers` before attempting decryption, freeing only in `Complete()`. Combined with a full-file 4KB probe, that suggested up to ~512K retained allocations on a 2GB journal, i.e. a plausible OOM.

**Actually measured** (test `EncryptedResyncScanMustNotRetainABufferPerProbedBoundary`, encrypted shared journal, 7,602,176 bytes = 1,856 blocks, 209 transactions, peak sampled by a polling thread *during* recovery):

| Scenario | Peak retained native memory |
|---|---|
| intact journal | 856,064 bytes |
| bypassing a foreign corrupted transaction | 864,256 bytes |
| **delta** | **8,192 bytes** (2 buffers) over 1,856 probed boundaries |

**My error:** the allocation sits *after* the `HeaderMarker` and bounds checks, so a boundary holding no transaction header costs nothing. Retention scales with the number of transaction *headers* in the scanned region, not with the number of boundaries - the same order as ordinary replay of that region.

Two methodology notes worth keeping:

- A before/after delta around the open always reads **zero**, because `Complete()` frees the buffers before the open returns. The peak has to be sampled by a concurrent thread.
- My first version of this test passed **vacuously**: it corrupted the measured environment's *own* transaction, so recovery aborted on the sequence gap and never scanned. Corrupt a *foreign* environment's transaction to exercise scan-then-continue.

Pre-existing property, not a 27278 regression, but worth knowing: encrypted recovery retains roughly one buffer per transaction in the journal simultaneously, so peak native memory during recovery is on the order of the journal file size (bounded by `Storage.MaxJournalFileSizeInMb`, default 2048).

## Downgraded: "the per-environment resync rescan is a startup-cost problem"

**Predicted:** every environment hard-linked to the file independently rescans from the corruption point to EOF with `EnsureMapped` and no `DiscardPages`, so N indexes means N full-file scans.

**Actually:** the structure is real, but each probe is a marker comparison that costs nothing when it misses - the same measurement above shows 1,856 probes adding no measurable time (whole two-fixture test runs in ~800ms). Extrapolated to a full 2GB journal that is ~512K probes per environment, which is worth knowing but is not the blowup I expected. **Not measured at production journal size** - if this ever needs settling, the recipe is a `StressTests` run at `MaxJournalFileSizeInMb=2048` with corruption at offset 0 and many index environments.

## Not a live issue: `Guid.Empty` JournalId adoption

**Predicted:** at [JournalReader.cs:693-696](../../../../src/Voron/Impl/Journal/JournalReader.cs) an environment whose `JournalId` is `Guid.Empty` adopts the id of the first transaction it validates. With a resync, a corrupted first transaction could shift that to a *sibling's* id, making the environment replay another environment's transactions.

**Actually:** the `Guid.Empty` path comes only from the `CurrentReadTransactionId == 1` constructor ([JournalReader.cs:63](../../../../src/Voron/Impl/Journal/JournalReader.cs)), whose sole caller is `IncrementalBackup.Restore` ([IncrementalBackup.cs:366](../../../../src/Voron/Impl/Backup/IncrementalBackup.cs)). There the adoption is *intentional* - the restored id is read back and written into the restored environment's header ([IncrementalBackup.cs:375, 409](../../../../src/Voron/Impl/Backup/IncrementalBackup.cs)). Every normally-created environment gets a fresh Guid at creation (`MetadataAccessor.cs:31`), so the field is never empty in the shared-journal path.

**Latent trap worth a comment, not a fix:** the restore switch has a `case ".merged-journal":` arm ([IncrementalBackup.cs:340](../../../../src/Voron/Impl/Backup/IncrementalBackup.cs)) and **nothing in the tree produces that extension**. If merged (shared) journals ever start being included in incremental backups, restoring one into a single environment would adopt whichever environment's transaction validates first and reconstruct the wrong environment's data - and with the resync, non-deterministically. Verified by search: zero producers today.
