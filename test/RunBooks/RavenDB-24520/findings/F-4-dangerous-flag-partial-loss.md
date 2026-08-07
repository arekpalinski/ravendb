# F-4 - CLOSED: the dangerous recovery flag no longer causes partial data loss

**Status: CLOSED, does not reproduce.** Re-run 2026-08-07 on the rebased branch (27156 / 27166 / 27168 / 27220 / 27278 / 26563). The flag now behaves exactly as documented and is genuinely useful.
**Evidence:** observed (4 cells, server level).
**Shared context:** see [README.md](README.md).

## What was originally claimed (pre-fix, 2026-07)

`--Storage.Dangerous.IgnoreInvalidJournalErrors=true` reduced the blast radius (1 index reset instead of 3) but salvaged indexes came up **silently short**: Questions/Search with 11,610 entries against a 13,857 baseline while reporting `State=Normal` and non-stale.

## Re-run result

Corruption is a 4-byte payload flip on a transaction owned by `Questions_Tags_ByMonths` in the active shared journal, run at two positions with and without the flag.

| Cell | target tx | flag | Questions/Tags/ByMonths | other 5 indexes | verify |
|---|---|---|---|---|---|
| `3A-noflag` | its **last** tx (txId 12, 1x4KB) | no | 10,744 | full | OK |
| `3A-ignoreflag` | its last tx | yes | 10,744 | full | OK |
| `3B-noflag-first` | a **mid-chain** tx (txId 7, 49x4KB, 200,110 B payload) | no | **0** - index failed to open, reset | full | PROBLEMS (expected) |
| `3B-ignoreflag-first` | same mid-chain tx | yes | **10,744** | full | OK |

Documents were 136,534 (= baseline) in all four. 10,744 is the uncorrupted baseline for this index, so **with the flag the index recovers completely** - no partial loss at all.

## Why the original observation is gone

**Index content is derived from documents.** Skipping a journal discards index-storage transactions, but the index then re-indexes the affected documents and converges to exactly the same entry count. That is why 3B-ignoreflag lands on 10,744 rather than something short.

Two candidate explanations for the original 11,610-vs-13,857 measurement, which I cannot distinguish retrospectively:

1. `dd66e0ac6c8` - "Publish the data pager state grown by a journal skipped via `IgnoreInvalidJournalErrors`" - fixed a real pager-state bug on **exactly this skip path**. If the skipped journal grew the data pager without publishing the new state, genuine loss was possible.
2. The pre-fix harness read entry counts before indexing had converged. Its settle logic was hardened afterwards to require 3 consecutive clean polls **and** more than 5 s.

Explanation 1 is squarely on the skip path and is the more likely of the two.

## Two behaviours worth knowing

**The flag is only reachable when the owner's own transaction chain breaks mid-stream.** Corrupting the owner's *last* transaction (3A) does not engage it at all: 27278's resync truncates the tail cleanly, recovery does not fail, and the flag has nothing to skip - both 3A cells are byte-identical. The flag engages only when a later own transaction exists after the damage, so the sequence check detects a gap and throws (3B).

**The no-flag message on the index path is good** - it names the index and gives two concrete remedies:

> Failed to open a storage at `...\Indexes\Questions_Tags_ByMonths` due to invalid or missing journal files. In order to load the storage successfully we need all journals to be not corrupted. The recommended approach is to **reset the index** in order to recover from this error. Alternatively you can temporarily start the server in **dangerous mode** so it will ignore invalid journals on startup.

Worth contrasting with the generic "Create a new database to recover" that surfaces on the root/init path - see [F-7](F-7-root-owned-corruption-blast-radius.md). The index path got this right; only the root path has bad guidance.

## Residual observation (reinforces F-6)

A single corrupted transaction produces a **FATAL `Index Recovery Error` alert for the root and for every index hard-linked to that journal** - 7 in cell 3A - even though only the owner is affected and, in 3A, nothing is lost at all. None of the seven messages says which index actually lost anything. Post-27278 the *outcome* is correctly isolated but the *alerting* is not. Folded into [F-6](F-6-linkrecord-bypass-diagnostics.md) rather than tracked here.

## Repro

```bash
# tail target - flag makes no difference, both clean
dotnet run --project test/Tryouts -c Release --no-build -- cell 3A-noflag payload Questions_Tags_ByMonths last shared

# mid-chain target - this is where the flag matters
dotnet run --project test/Tryouts -c Release --no-build -- cell 3B-noflag-first payload Questions_Tags_ByMonths first shared
$env:RAVEN_24520_EXTRA_ARGS='--Storage.Dangerous.IgnoreInvalidJournalErrors=true'
dotnet run --project test/Tryouts -c Release --no-build -- cell 3B-ignoreflag-first payload Questions_Tags_ByMonths first shared
$env:RAVEN_24520_EXTRA_ARGS = $null
```

Note the `Skipping this journal` confirmation goes to `addToInitLog` (the database-load init log surfaced in the Studio), **not** to `server.log`, so its absence from the NLog output is not evidence that the skip did not happen. Judge from the outcome: index opens with full entries instead of being reset to 0.
