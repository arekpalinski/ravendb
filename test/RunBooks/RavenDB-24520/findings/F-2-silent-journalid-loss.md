# F-2 - JournalId-field corruption at the tail: clean load, no error, silent transaction drop

**Severity:** minor (narrow trigger - one specific 16-byte field, at the tail - but zero integrity signal)
**Status:** open - probably a note on RavenDB-24520 rather than its own bug.
**Evidence:** observed.
**Shared context:** see [README.md](README.md).

## Claim

Corrupting the `JournalId` field (offset 136, 16 bytes) of the last transaction in a shared journal produces a completely clean startup - the DB loads, no recovery error is logged, no index is faulted - but that transaction is silently reassigned to a bogus environment and dropped. Its real owner never replays it, so a committed change is silently lost with no signal at all.

## Mechanism

The transaction payload hash (`ValidatePagesHash`) is computed over the payload bytes, not over the header's `JournalId` field. Flipping `JournalId` therefore leaves the hash valid. During recovery every environment sees a valid transaction whose `JournalId` matches neither its own nor `Guid.Empty` (legacy) nor the `LinkedJournalsRecord` sentinel, so all of them skip it as "not mine" (`JournalReader.TryReadAndValidateHeader` owner-filter). Because it is the last transaction, no env has a later valid tx of its own after it, so there is no `InvalidJournalException` (contrast [F-1](F-1-no-isolation.md)'s cascade for a mid-file corruption). Net: the tx is dropped by everyone, cleanly.

## Evidence (observed)

Cell `1C-journalid` (payload-hash-valid `JournalId` flip on the tail tx of the active journal):
- DB loaded, all 6 indexes State=Normal, entries unchanged, **0 recovery errors logged**.
- The flipped tx (its owner's last commit) is not applied anywhere.

Contrast: the same flip on a **non-tail** tx would make the owner see a tx-sequence gap and throw (F-1 territory), which at least surfaces an error.

## Repro

```bash
dotnet run --project test/Tryouts -c Release --no-build -- cell 1C-journalid journalid Questions_Search last shared
# -> "clean recovery", no errors; the corrupted tx is silently skipped by every env
```
(`journalid` op XORs the 16-byte `JournalId` at header offset 136; `last` targets the tail tx; `shared` = the active shared journal.)

## Open decision

- Real-world likelihood is low (requires that exact field to flip while the payload stays hash-valid, at the tail), so severity is minor.
- Worth a defensive integrity signal? e.g. treat a valid-hash tx whose `JournalId` matches no known environment as suspicious and at least log it, rather than silently skipping. Discuss whether that's worth the code in the hot recovery loop. Note the header hash covers only the payload, so a cheap alternative (hash the header too) is a format change and out of scope here.
