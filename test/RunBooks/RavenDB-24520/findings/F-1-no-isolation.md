# F-1 - No per-index isolation: a corrupted transaction faults every index sharing the journal

**Severity:** design / blast-radius (data is not lost, but damage crosses index boundaries)
**Status:** open - not filed yet, needs a decision on whether to change hot recovery code.
**Evidence:** observed (13-cell matrix + deterministic Voron test) + derived (code path).
**Shared context:** see [README.md](README.md). This is the root cause behind ticket answers Q1.1 and Q2.2 (see [ticket-answers.md](ticket-answers.md)), and it is why [F-3](F-3-write-failure-access-violation.md)'s blast radius was the whole database.

## Claim

A single corrupted (or write-failed) transaction in a shared journal faults recovery for **every** index environment hard-linked to that physical journal file - not just the index that owns the transaction. There is no per-index isolation.

## Mechanism / root cause

On recovery, each environment replaying the shared journal validates the XXHash64 of **every** transaction in the file **before** applying the per-env `JournalId` owner-filter.

In `JournalReader.TryReadAndValidateHeader` (`src/Voron/Impl/Journal/JournalReader.cs`):
1. `TryValidateTransaction` -> `ValidatePagesHash` runs first (hash check) for the transaction under the cursor, regardless of owner.
2. Only after a tx validates does the code skip it when `current->JournalId != this-env's JournalId` (the owner-filter).

So a transaction owned by index X that fails its hash makes index Y's recovery throw (`InvalidJournalException` via `VerifyNoUnexpectedValidTransactionsAfter`, or an `OnRecoveryError`), because Y validates X's bytes before it would have skipped them.

**Blast radius = the set of environments hard-linked to that physical journal file:**
- Corrupt a tx in the **active** journal (linked by all 6 indexes) -> up to all 6 indexes fault. (observed: cells 1A, 1C, 1E)
- Corrupt a tx in an **old** journal (linked by 1 index) -> only that 1 index faults. (observed: cell 1A-synced-first -> only Questions_Tags)

## Behavior by corruption kind (observed, Scenario 1 matrix)

| Corruption | Result |
|---|---|
| payload byte flip / Hash-field / zeroed 4KB block | hash mismatch -> `InvalidJournalException` -> all linked indexes fault |
| header-marker smash, **mid-file** | premature-EOF look, but valid later txs -> `InvalidJournalException` -> linked indexes fault |
| header-marker smash / truncation, **tail tx** | clean - treated as end-of-journal, all indexes intact |
| link-record (`LinkedJournalsRecord`) payload | hash mismatch -> cascades like any tx (not special) |
| delete active journal file / diverge (break inode, same content) | clean - multiply-hard-linked; content survives via other links |
| `JournalId`-field flip, tail tx | clean but silent tx drop -> see [F-2](F-2-silent-journalid-loss.md) |

Documents are never lost (separate env); the DB still loads. Damage is confined to index envs (which then reset - see Q1.2 in [ticket-answers.md](ticket-answers.md)).

## Repro

Server-level (blast radius across real indexes):
```bash
dotnet run --project test/Tryouts -c Release --no-build -- cell 1A payload Questions_Tags_ByMonths last shared
# -> multiple indexes go to State=Error though only one index's tx was corrupted
```
A deterministic Voron-level version (root + 2 branches sharing one journal, corrupt branch A's first tx, assert the **root** - a different env - chokes on it) was written during the campaign but not kept; it is straightforward to re-add if F-1 is filed.

## Open decision

- Is the current behavior acceptable (documents safe, indexes rebuildable) or worth changing?
- Candidate change (hot code, plan required): a branch replaying the shared journal could skip / tolerate hash failures on transactions whose `JournalId` isn't its own, limiting damage to the owning index. Risk: the owner-filter currently runs after validation for a reason (sequence checks, `LinkedJournalsRecord` handling, legacy `Guid.Empty` txs) - need to confirm reordering is safe for the root's own recovery and for tx-sequence verification.
- Cheaper alternative: leave the behavior, document it, and make the operator-facing message say explicitly that *all* indexes sharing the journal need a reset (today each faulted index reports individually).
