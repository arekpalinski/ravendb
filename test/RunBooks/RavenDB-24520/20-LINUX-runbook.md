# RavenDB-24520 Linux Runbook

Re-run of the campaign on Linux. Read [00-REFERENCE.md](00-REFERENCE.md) first. The harness is OS-portable; only paths, the disk-full mechanism, and a couple of Voron behaviors differ.

## Setup

```bash
export RAVEN_24520_BASE=/tmp/24520
export RAVEN_24520_DUMPS=/path/to/stackoverflow-data-small
export RAVEN_24520_INDEXES=/path/to/stackoverflow-data/SO-indexes.ravendbdump
dotnet build src/Raven.Server -c Release
dotnet build test/Tryouts -c Release
```

The harness auto-detects Linux for hard-link creation (`link(2)`), inode identity (`stat -c %d:%i`), and link counts (`stat -c %h`). `FindServerDll` locates `src/Raven.Server/bin/Release/net10.0/Raven.Server.dll` by walking up to the repo root (`RavenDB.slnx`). `stat` must be on PATH (coreutils - present on any normal distro).

## Read this first - what is already settled (do not re-chase it)

State as of 2026-08-07 on the rebased branch (27156 / 27166 / 27168 / 27220 / 27278 / 26563):

| Finding | Status |
|---|---|
| F-1 no per-index isolation | **FIXED** by RavenDB-27278 (resync + per-env sequence attribution). Its position rule is obsolete |
| F-3 write-failure ACCESS_VIOLATION | **FIXED** by 27156 + 27166. Re-validated: 14/14 AV loop, and a real ENOSPC that reached the merged write |
| F-4 dangerous flag partial loss | **CLOSED**, does not reproduce - salvaged index recovers to the exact baseline |
| F-5 JournalId impersonation | **WITHDRAWN** - constructed scenario |
| F-7 root blast radius | **REFUTED** at server level |
| F-2, F-6 | open, both minor diagnostics |

**Two methodology traps that cost this campaign two false findings - do not repeat them:**

1. **Sync state governs reachability.** Voron-level fixtures that set `ManualFlushing`/`ManualSyncing` never sync, so recovery fully validates every transaction. A real environment syncs promptly and then **skips already-synced transactions without validating them** (`JournalReader.IsAlreadySyncTransaction`). The victims a test naturally picks - early transactions, the initializing transaction, the head of the file - are exactly the ones production has synced past. Always re-test at server level before believing a corruption finding.
2. **Do not grep for `"CatastrophicFailure state, about to throw"`** to decide whether the 27156 poisoning fired. That string is `AssertNoCatastrophicFailure` complaining when something *later uses* a poisoned environment and can be absent on a run where the fix worked perfectly. Look instead for `SharedJournalState.SetException` / `MarkCatastrophicFailure` in the `CatastrophicFailureHandler` stack, plus a **branch** env named in the unload message.

## Post-fix retest (do this first)

The Windows run confirmed 27156 + 27166 + 27278 (28/28 in `FastTests.Voron.SharedJournal`, 9/9 in the SlowTests fix classes, 14/14 AV-loop runs clean vs a ~40-60% pre-fix crash rate). Repeat on Linux:

- [ ] `dotnet test test/FastTests -c Release --filter "FullyQualifiedName~FastTests.Voron.SharedJournal"` -> expect all green (includes `RavenDB_27278` and the campaign's `RavenDB_24520` characterization tests).
- [ ] `dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~RavenDB_27156|FullyQualifiedName~RavenDB_27166|FullyQualifiedName~RavenDB_27278|FullyQualifiedName~RavenDB_24520_e2e"` -> expect all green.
- [ ] Loop the AV repro >=10x, expect 0 crashes (a crash on Linux would show as exit 139 / SIGSEGV rather than `0xC0000005`):
```bash
for i in $(seq 1 12); do
  dotnet test test/SlowTests -c Release --no-build \
    --filter "FullyQualifiedName~RavenDB_27156_e2e.TornJournalWrite_OnSharedRoot" > /tmp/24520/postfix-$i.log 2>&1
  echo "run $i : exit=$?"
done
```
  Capture dumps if one crashes: `export DOTNET_DbgEnableMiniDump=1 DOTNET_DbgMiniDumpType=2`.

## Phase 0 / Scenario 1

Identical commands to the [Windows runbook](10-WINDOWS-runbook.md) - `seed`, `map`, `restore-work`, `verify`, and every `cell ...`. The corruption ops are byte-level and OS-independent.

Linux-specific things to watch and record:
- ext4's hard-link cap is ~65000 vs NTFS ~1023, so the hard-link-limit fallback path (RavenDB-24069) triggers at a very different scale. Not exercised by this campaign; noted for completeness.
- Case-sensitive filesystem: index dir names keep their exact case. The `@SharedJournals` prefix match is unaffected.
- Compare each cell's outcome against the Windows findings table; any divergence is itself a finding.

## Scenario 1G / Scenario 3 - corrupt while the server is RUNNING (Linux-only opportunity)

On Windows the journal files are opened with restrictive share modes, so an external process usually cannot rewrite them mid-run. Linux has no mandatory locking, so we can corrupt a journal while the server holds it open and observe live detection.

- [ ] Start a server on `work` (`... -- server /tmp/24520/work &`), let indexing run.
- [ ] While it runs, flip bytes in an active `@SharedJournals` journal (same offsets `JournalTools` uses; add a `corrupt-live` harness command if convenient).
- [ ] Observe: does the running server detect it (next flush/read), or only on restart? Data integrity afterwards? Does it surface as a wrong-page `VoronUnrecoverableErrorException`?

## Scenario 2 - real disk-full on Linux (loop device)

Preferred: an ext4 filesystem on a loop device. Needs `sudo` once to mount.

**Size this correctly or the run is meaningless.** The original version of this section said 512 MB with `RAVEN_24520_SAMPLE=500`, which cannot work: Voron preallocates journals and scratch, so a small dataset's index rebuild needs no new allocation and produces **no ENOSPC at all**. Three Windows runs at 40 MB / 8 MB / even 1 MB free finished with zero disk-full entries for exactly this reason - inconclusive, not passes. Use the ~130k-doc golden (`SAMPLE=50`) and a volume big enough to hold golden + work (~1.5 GB) plus headroom.

```bash
dd if=/dev/zero of=/tmp/rdb-diskfull.img bs=1M count=6144   # 6 GB, NOT 512 MB
mkfs.ext4 -q /tmp/rdb-diskfull.img
mkdir -p /tmp/rdb-diskfull
sudo mount -o loop /tmp/rdb-diskfull.img /tmp/rdb-diskfull
sudo chown "$USER" /tmp/rdb-diskfull
export RAVEN_24520_BASE=/tmp/rdb-diskfull/24520
export RAVEN_24520_SAMPLE=50                                 # ~130k docs, NOT 500
```

Then:
- [ ] `... -- seed 2` (golden on the loop volume, so hard links form there)
- [ ] `... -- restore-work`
- [ ] `... -- diskfull /tmp/rdb-diskfull/24520/work 100` -> expect `VERDICT: ENOSPC reached and handled gracefully`
- [ ] `... -- verify /tmp/rdb-diskfull/24520/work` after the balloon is freed -> expect exact recovery, no data loss

Expected (matching Windows 2026-08-07): graceful `DiskFullException` (ENOSPC) -> `SharedJournalState.SetException` poisons every participant -> one `CatastrophicFailureHandler` DB unload -> **server alive**, then exact recovery. A process crash (exit 139 / SIGSEGV on Linux) would mean F-3 regressed.

**Where ENOSPC actually lands is a race.** On a preallocated journal a write into already-allocated space cannot fail, so real disk-full surfaces at an *allocation*: creating a journal, growing a journal, or growing a compression buffer. Only the first two sit inside the root's merged write and therefore exercise 27156; a compression-buffer failure happens in a branch's own pre-merge preparation and is simply retried by `MapIndex.HandleDiskFullErrors`. Both Windows runs happened to land in the merged write (via `NextFile` and via "Failed to increase file"), but do not treat a real disk-full run as *the* validation for 27156 - the deterministic one is the AV loop above.

`diskfull` clears `LogsDir` on entry (fixed 2026-08-07). Before that fix, a repeated run in the same base dir counted the previous run's log entries and could report `ENOSPC reached and handled gracefully` for a run that never filled the disk. If you see that verdict with a suspiciously low entry count, confirm against timestamps in `$RAVEN_24520_BASE/logs/server.log`.

Cleanup: `sudo umount /tmp/rdb-diskfull && rm /tmp/rdb-diskfull.img`, and unset the env vars.

Alternative without sudo: rely on the injected `SimulatePartialJournalWriteFailure` seam (the committed e2e) for the torn-write path; the real-volume-full path then differs only in the storage-space monitor, which can be recorded as Windows-verified.

## Notes / observations (fill in)

(Linux-specific differences vs the Windows findings go here.)
