# F-4 - Dangerous recovery flag salvages more indexes but with silent partial data loss

**Severity:** minor / documentation (the flag is explicitly "Dangerous"; this documents its exact behavior for shared journals)
**Status:** open - candidate note on RavenDB-24520 + possibly a docs clarification.
**Evidence:** observed.
**Shared context:** see [README.md](README.md).

## Claim

`--Storage.Dangerous.IgnoreInvalidJournalErrors=true` lets a database with a corrupted shared journal start by **skipping** the invalid journal instead of faulting. It reduces the blast radius (fewer indexes reset) and avoids a manual reset, but the indexes whose skipped journal held real transactions come up with **silently fewer entries** while reporting State=Normal / non-stale - i.e. silent partial data loss, no staleness or error signal.

## Evidence (observed)

Same payload corruption (active shared journal), run twice:

| | indexes reset | notable |
|---|---|---|
| without flag | 3 reset to entries=0 | normal blast radius ([F-1](F-1-no-isolation.md)) |
| with `IgnoreInvalidJournalErrors=true` | 1 reset | Questions/Search came up with **11,610** entries vs **13,857** baseline, State=Normal, non-stale |

So the flag skipped the invalid journal and salvaged most indexes, but the salvaged ones can be silently short of data.

## Repro

```bash
# baseline (no flag)
dotnet run --project test/Tryouts -c Release --no-build -- cell 3A-noflag payload Questions_Tags_ByMonths last shared

# with the dangerous flag (harness appends RAVEN_24520_EXTRA_ARGS to the server args)
$env:RAVEN_24520_EXTRA_ARGS='--Storage.Dangerous.IgnoreInvalidJournalErrors=true'
dotnet run --project test/Tryouts -c Release --no-build -- cell 3A-ignoreflag payload Questions_Tags_ByMonths last shared
Remove-Item Env:\RAVEN_24520_EXTRA_ARGS
```

## Assessment

This is arguably correct-by-design for a flag named "Dangerous": it trades completeness for availability, and the startup error message already tells operators to reset the affected indexes afterwards. The finding is that for shared journals the partial loss is **silent** (Normal / non-stale), so an operator who uses the flag and doesn't follow the reset advice won't notice missing index entries.

## Open decision

- Keep as-is (documented dangerous behavior) or add a louder post-recovery signal (e.g. mark the indexes whose journal was skipped so they surface "recovered with skipped data, reset recommended")? Low priority.
