# F-6 - A bypassed LinkedJournalsRecord skips hard-link repair, and no message says so

**Severity:** low - diagnostics only. **No data loss, no silent degradation.** Behaviour is safe; the complaint is that an operator cannot connect the two errors they see.
**Status:** open, minor. Found 2026-08-07 on the rebased branch.
**Evidence:** observed (characterization test, both variants).
**Shared context:** see [README.md](README.md).

## What I predicted, and what is actually true

This started as a hypothesis that the RavenDB-27278 resync would make the loss of a `LinkedJournalsRecord` **silent**, on two grounds: the record is protected by no transaction-id sequence check, and `TryFindNextValidTransaction` disables both `OnRecoveryError` and `OnIntegrityErrorOfAlreadySyncedData` for the duration of the scan.

**The first half is true, the second half is wrong.** The handler suppression covers only the forward *scan*; the transaction that first fails validation is rejected in the main loop with the handlers still live, so a recovery error is raised normally. Measured, not assumed - the test collects the handler callbacks and gets two of them.

So the "completely silent bypass" hypothesis is **refuted**. Recording it because it was on the plan as a likely finding.

## Confirmed mechanism

The link record carries the sentinel `LinkedJournalId` (`66d2ff9c-6251-462c-bde5-e05ba50110cf`) and is handled at [JournalReader.cs:698-704](../../../../src/Voron/Impl/Journal/JournalReader.cs) with a `continue` placed **before** `VerifyTransactionSequence`. It therefore belongs to no environment's transaction-id chain, and nothing detects that it went missing. Corrupt it and the resync bypasses it, so `ProcessLinkedJournalsRecord` never runs and the hard links it would have re-created stay missing.

## Observed behaviour

Test: `FastTests.Voron.SharedJournal.RavenDB_24520.BypassedLinkRecordSkipsHardLinkRepairButFailsLoudly`. Baseline and corrupted variants, where the scenario is the exact machine-level failure the record exists to repair - branch B's hard link is deleted.

| Variant | Hard link restored | Errors reported | Branch B afterwards |
|---|---|---|---|
| intact link record (baseline) | **yes** | 0 | opens, data intact |
| corrupted link record | **no** | 2 (`Invalid hash signature for transaction ... JournalId: 66d2ff9c-...`) | fails loudly: `InvalidJournalException: No such journal '...\0000000000000000000.journal'` |

The baseline passing matters: it proves the fixture really exercises the repair path, so the corrupted row is meaningful.

At server level a recovery error is not swallowed - `DocumentDatabase.HandleOnRecoveryError` logs it at **Fatal** and raises an `AlertRaised` with `NotificationSeverity.Error` ([DocumentDatabase.cs:2147-2157](../../../../src/Raven.Server/Documents/DocumentDatabase.cs)). The affected index then fails to open, which for an index means faulty + rebuild - the acceptable outcome.

## The actual (small) complaint

An operator sees two unrelated-looking things:

1. `Index Recovery Error - <shared journals>`: "Invalid hash signature for transaction ... JournalId: 66d2ff9c-6251-462c-bde5-e05ba50110cf". That Guid is an internal sentinel with no documented meaning, so nothing indicates a *hard-link repair record* was destroyed.
2. A separate index failing with "No such journal".

Nothing links them, and nothing states the actionable fact: a journal-link repair record was lost, so branch X's journal link could not be re-created. Two cheap improvements:

- When `ProcessLinkedJournalsRecord` is bypassed, log a Warn naming the record and the consequence. This is the same one-line resume log that would also cover the general "recovery resumed past a bypassed region at [from-to)" case - `_resyncedFromInvalid4KbPosition` / `_resyncedToValid4KbPosition` are already tracked but only ever used to decorate exception messages that are not thrown when the bypass is judged safe.
- Render the sentinel `JournalId` as something readable (e.g. "LinkedJournals repair record") in `TransactionHeader.ToString()` rather than a raw Guid.

Neither is urgent.

## Related, measured under F-4 on 2026-08-07: the alerting is not isolated even though the outcome is

A single corrupted transaction in a shared journal raises a **FATAL `Index Recovery Error` alert for the root and for every index hard-linked to that file** - 7 of them in cell `3A-noflag` - because each environment encounters the invalid transaction during its own scan and calls `InvokeRecoveryError` before resyncing past it. In that cell **nothing was actually lost by anyone**: all 6 indexes came up Normal with full entry counts and `verify` reported OK.

So post-27278 the *recovery outcome* is correctly isolated to the owner, but the *operator-facing signal* still fans out to everything sharing the journal, and none of the seven messages indicates which index (if any) actually lost data. An operator sees seven FATAL alerts naming seven indexes for one harmless corrupted tail transaction.

Same cheap fix as above would help: log the resume decision (which environment, which region bypassed, whether its own chain was intact) so the one message that matters is distinguishable from the six that do not.
