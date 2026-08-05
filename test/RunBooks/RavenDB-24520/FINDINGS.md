# RavenDB-24520 Findings

Split into one self-contained file per finding under [`findings/`](findings/) so each can be discussed in isolation: "read `test/RunBooks/RavenDB-24520/findings/<file>` and let's go over it."

Start at [findings/README.md](findings/README.md) for the index + shared context (environment, harness, key code map, regression tests).

## Headlines

1. **An IO error / ENOSPC at the shared-journal write could crash the whole server process** (ACCESS_VIOLATION, ~40-60% of runs). Root cause: a failed write left the branch envs' scratch state corrupted while only the root env was poisoned. **FIXED** under RavenDB-27156 + RavenDB-27166. [findings/F-3](findings/F-3-write-failure-access-violation.md)
2. **No per-index isolation** on journal corruption or write failure - one corrupted/failed transaction faults every index sharing that physical journal. Filed as RavenDB-27278, fix implemented on its branch. [findings/F-1](findings/F-1-no-isolation.md)
3. **Documents are never lost** - damage is confined to index environments, which reset/rebuild; the DB always loads.

## Findings

| ID | Severity | Status | File |
|----|----------|--------|------|
| F-3 | serious | FIXED (27156 + 27166) | [findings/F-3-write-failure-access-violation.md](findings/F-3-write-failure-access-violation.md) |
| F-1 | design | filed as RavenDB-27278, fix on its branch | [findings/F-1-no-isolation.md](findings/F-1-no-isolation.md) |
| F-2 | minor | open | [findings/F-2-silent-journalid-loss.md](findings/F-2-silent-journalid-loss.md) |
| F-4 | minor | open (by design?) | [findings/F-4-dangerous-flag-partial-loss.md](findings/F-4-dangerous-flag-partial-loss.md) |
| ticket Q&A | - | - | [findings/ticket-answers.md](findings/ticket-answers.md) |
