# RavenDB-24520 Findings

Split into one self-contained file per finding under [`findings/`](findings/) so each can be discussed in isolation: "read `test/RunBooks/RavenDB-24520/findings/<file>` and let's go over it."

Start at [findings/README.md](findings/README.md) for the index + shared context (environment, harness, key code map, regression tests). That file is the authoritative status list; this one is the summary.

Status as of 2026-08-10, on the rebased branch (27156 / 27166 / 27168 / 27220 / 27278 / 26563), verified on **both Windows and Linux**.

## Headlines

1. **An IO error / ENOSPC at the shared-journal write could crash the whole server process** (ACCESS_VIOLATION, ~40-60% of runs). Root cause: a failed write left the branch envs' scratch state corrupted while only the root env was poisoned. **FIXED** under RavenDB-27156 + RavenDB-27166; re-validated 14/14 on Windows and 12/12 on Linux with zero crashes. [findings/F-3](findings/F-3-write-failure-access-violation.md)
2. **No per-index isolation** on journal corruption - one corrupted transaction faulted every index sharing that physical journal. **FIXED** by RavenDB-27278 (resync + per-env sequence attribution): a corrupted transaction now faults exactly its owner. [findings/F-1](findings/F-1-no-isolation.md)
3. **Documents are never lost.** Damage is confined to index environments, which reset and rebuild. Held in every cell of both 16-cell matrix runs and after every disk-full run.
4. **One case still fails the whole database**: a missing *old* journal in the root's `@SharedJournals/Journals/` directory. Fully recoverable with `Storage.Dangerous.IgnoreInvalidJournalErrors=true`, but the error offered no hint of that. Diagnostics half filed as **RavenDB-27293**. [findings/F-9](findings/F-9-missing-root-journal-fails-database.md)

## Findings

| ID | Severity | Status | File |
|----|----------|--------|------|
| F-3 | serious | **FIXED** (27156 + 27166) | [findings/F-3-write-failure-access-violation.md](findings/F-3-write-failure-access-violation.md) |
| F-1 | design | **FIXED** (27278) - its position rule is obsolete | [findings/F-1-no-isolation.md](findings/F-1-no-isolation.md) |
| F-9 | medium-low | diagnostics half **filed as RavenDB-27293** (PR open); rest unfiled | [findings/F-9-missing-root-journal-fails-database.md](findings/F-9-missing-root-journal-fails-database.md) |
| F-2 | minor | open | [findings/F-2-silent-journalid-loss.md](findings/F-2-silent-journalid-loss.md) |
| F-6 | low | open (nit) | [findings/F-6-linkrecord-bypass-diagnostics.md](findings/F-6-linkrecord-bypass-diagnostics.md) |
| F-4 | - | **CLOSED** - does not reproduce | [findings/F-4-dangerous-flag-partial-loss.md](findings/F-4-dangerous-flag-partial-loss.md) |
| F-5 | - | **WITHDRAWN** - constructed scenario | (deleted, reasoning in [F-8](findings/F-8-refuted-hypotheses.md)) |
| F-7 | - | **REFUTED** at server level; holds the sync-state methodology lesson | [findings/F-7-root-owned-corruption-blast-radius.md](findings/F-7-root-owned-corruption-blast-radius.md) |
| F-8 | - | closed - negative results from the resync review | [findings/F-8-refuted-hypotheses.md](findings/F-8-refuted-hypotheses.md) |
| ticket Q&A | - | current (rewritten against the rebased branch) | [findings/ticket-answers.md](findings/ticket-answers.md) |

## Platform coverage

| | Windows | Linux |
|---|---|---|
| Post-fix suites | 28/28 FastTests, 9/9 SlowTests fix classes | 28/28, 10/10 (one test newer) |
| F-3 AV loop | 14/14, 0 crashes | 12/12, 0 crashes |
| 16-cell corruption matrix | POST-27278 re-baseline | **identical, 16/16** |
| Real disk-full | graceful, 27156 poisoning fired | graceful, exact recovery, **poisoning not exercised** (race landed on branch data-pager growth) |
| Corrupt a journal held open by a live server | not possible (share modes) | **done** - no live detection, damage owner-only, no new finding |

Full Linux detail, including three harness bugs that made Linux runs look like passes, is in [20-LINUX-runbook.md](20-LINUX-runbook.md).

**No behavioral divergence between the two platforms was found.** Every issue surfaced by the Linux pass was in the test harness, not the product.
