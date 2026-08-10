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

- [x] `dotnet test test/FastTests -c Release --filter "FullyQualifiedName~FastTests.Voron.SharedJournal"` -> **28/28 passed** (11 s), same count as Windows.
- [x] `dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~RavenDB_27156|FullyQualifiedName~RavenDB_27166|FullyQualifiedName~RavenDB_27278|FullyQualifiedName~RavenDB_24520_e2e"` -> **10/10 passed** (1 m 18 s). The Windows line records 9/9 for "the fix classes"; the filter now also catches `RavenDB_24520_e2e.DamagedSharedRoot_...`, which post-dates that note. Test *inventory* difference, not a behavior difference - the 10 are 4x `RavenDB_27156`, 3x `RavenDB_27166`, `RavenDB_27156_e2e.TornJournalWrite_OnSharedRoot_...`, `RavenDB_27278_e2e.CorruptedTxOfSingleIndex_...`, `RavenDB_24520_e2e.DamagedSharedRoot_...`.
- [x] Loop the AV repro >=10x, expect 0 crashes (a crash on Linux would show as exit 139 / SIGSEGV rather than `0xC0000005`): **12/12 PASS, 0 crashes** (2026-08-10). No exit >128 and no `SIGSEGV` / `Fatal error` / aborted-run marker in any run's log - `dotnet test` can report a child crash as a plain exit 1, so check both.
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

### LINUX RESULT (2026-08-10): all 16 cells identical to the Windows POST-27278 RE-BASELINE

Seed and baseline are byte-comparable with the Windows run, which makes the comparison meaningful rather than approximate: **130,534 docs / 136,534 after burst**, 6 indexes, **9 inode groups / 26 files**, active journal `...009.journal` linked by all 6 branches + the root (`links=7`). `restore-work` re-created 17 links across 9 groups via `link(2)`. Phase 0 `verify` clean, with every entry count matching Windows exactly (Activity/ByMonth 129, Questions/Search 13,857, Questions/Tags 5,350, Questions/Tags/ByMonths 10,744, Users/Registrations/ByMonth 129, Users/Search 122,677).

| Cell | Windows POST-27278 | Linux 2026-08-10 |
|---|---|---|
| 1A-branch-unsynced | clean, 0 resets | clean |
| 1A-synced-first | owner only - Questions/Tags entries=0 | same |
| 1A-root | clean | clean |
| 1A-other-env | owner only - Users/Search entries=0 | same |
| 1B-marker-tail | clean | clean |
| 1B-marker-mid | owner only - Users/Search entries=0 | same |
| 1C-hash | clean | clean |
| 1C-txid | clean | clean |
| 1C-journalid | clean | clean |
| 1D-zeroblock | clean | clean |
| 1D-trunc-tail | clean | clean |
| 1D-trunc-mid | clean | clean |
| 1E-linkrec | clean | clean |
| 1F-delete-active | clean | clean |
| **1F-delete-old** | **WHOLE DATABASE FAILS TO LOAD** (F-9) | same |
| 1F-diverge | clean | clean |

**No divergence, so no new finding from Scenario 1.** No server process died in any cell. Every clean cell recovered to the exact baseline (136,534 docs, all six entry counts unchanged), so the three POST-27278 rules hold unchanged on Linux: last-transaction corruption is benign whatever the kind, a transaction with later own transactions faults exactly its owner, and file-level topology games are clean.

**Independent confirmation of a Windows conclusion.** This seed's journal layout differs from the Windows one - `Questions_Search last` is txId=17 at offset 13,197,312 here vs 11,571,200 there, and the per-cell target offsets differ throughout - yet every outcome is identical. That is direct evidence for the Windows claim that the dead pre-fix "position rule" reflected layout-dependent victim selection rather than behavior.

## Scenario 1G / Scenario 3 - corrupt while the server is RUNNING (Linux-only opportunity)

On Windows the journal files are opened with restrictive share modes, so an external process usually cannot rewrite them mid-run. Linux has no mandatory locking, so we can corrupt a journal while the server holds it open and observe live detection.

Implemented as a harness command (2026-08-10):

```
corrupt-live <name> <op> <ownerFilter> <which> [fileSelector] [--probe passive|reload|both] [--observe <sec>]
```

It restores `work` from golden, starts a server, primes and settles indexing (so the target tx is fully committed rather than half-written), records the target's sync state, corrupts the open file, watches, then hard-kills and verifies. `op` / `ownerFilter` / `which` / `fileSelector` mean exactly what they do for `cell`, so a live run is directly comparable to its at-rest cell.

**What the code predicts before running anything.** A running environment never reads journal bytes: flush pushes pages from scratch to the data file (`WriteAheadJournal.ApplyPagesToDataFileFromScratch`), and the only journal readers are recovery (`Options.OpenJournalPager` in the `WriteAheadJournal` recovery loop) and `IncrementalBackup`. Journal writes are positional fd writes (`Pal.rvn_write_journal`), not a shared mmap. And nothing locks journal files on Linux - the only `flock` in the tree guards the secret-key file (`SecretProtection.cs`). So the corruption always *succeeds*, and passive live detection should be impossible.

**Three traps this scenario walks into. All three produce a clean result for the wrong reason.**

1. **Do not probe with an index `RESET`.** It looks like the obvious way to force a live branch recovery, but a reset index gets a **fresh `JournalId`**, so post-27278 the owner filter skips every pre-existing transaction unvalidated - the corrupted one included. Such a probe cannot detect anything by construction, *and* it destroys what the restart phase measures by making the damaged tx foreign. Use a database **disable + enable** instead: that reloads every environment under its original `JournalId`.
2. **Re-check the target's sync state immediately before the restart, not just at corruption time.** A live server syncs while you observe. A target that was unsynced when corrupted can be synced past by restart time, after which recovery skips it unvalidated (`JournalReader.IsAlreadySyncTransaction`) and the run reports a meaningless "clean". The first 1G run on this box lost ~90 s to observation and hit exactly this.
3. **A graceful unload flushes and syncs.** Anything that stops the database politely - including `disable` - promotes the corrupted transaction to already-synced before the next recovery reads it. Only a hard kill (SIGKILL, which `ServerProcess.Kill` does) leaves the journal as the authoritative copy of those transactions.

Taken together: the journal is only ever the source of truth for transactions that are **written but not yet synced**, and only until something syncs them. That window is what live corruption can actually damage, and any 1G run has to prove it stayed inside it.

### LINUX RESULT (2026-08-10): live corruption succeeds, is never detected live, and is bounded exactly as at rest

Three runs, all `payload Users_Search last shared` on the active shared journal (`...011.journal`, held open by a live server, `links=3`).

| Run | Probe | Target still unsynced at restart? | Live detection | After hard-kill restart |
|---|---|---|---|---|
| `1G-live-payload-users-last` | passive + reset (flawed, see trap 1) | no - synced during ~90 s of observation | none | clean (**meaningless** - reset had re-created the owner with a fresh `JournalId`) |
| `1G-reload` | passive + db disable/enable | **file was deleted** - synced and reclaimed | none, incl. across a full in-process reload | clean (**meaningless** - the corrupted bytes no longer existed) |
| **`1G-passive-short`** | passive, 5 s window | **yes** (`lastSyncedJournal=5 lastSyncedTx=31`, target `txId=36`) | none | **owner only: Users/Search `entries=0`** + `IndexOpenException`; all 5 siblings, the root and documents intact; docs 144,534 (exact) |

**Conclusions.**

1. **The corruption always succeeds.** Every run flipped 4 payload bytes of `txId=36` inside a journal a running server held open, with no error and no locking obstacle. This step is simply unavailable on Windows, where the share modes reject the write.
2. **A running server never notices - not even during a full in-process recovery.** No detection passively, and none when `disable`+`enable` reloaded every environment under its original `JournalId`. Zero `VoronUnrecoverableErrorException` / `SetException` / `MarkCatastrophicFailure` / `InvalidJournalException` markers in the log while running, and the process stayed alive throughout. This is exactly what the code predicts: journals are write-only while an environment is up.
3. **The blast radius is identical to the at-rest matrix.** With the corrupted tx still unsynced at restart, recovery faulted **exactly its owner** - the same "owner only" class as the at-rest `1A-other-env` and `1B-marker-mid` cells - with no cross-index cascade, no database failure and no crash. **So Scenario 1G produces no new finding: live corruption is not more dangerous than corruption at rest.**
4. **The exposure window is narrow and self-closing.** Only written-but-unsynced transactions are vulnerable, and only until the next sync. Background sync closes it on its own; a graceful unload closes it immediately; and once a journal is fully synced Voron reclaims it, deleting the corrupted bytes altogether (that is why `1G-reload`'s target file was gone by restart).

The practical reading for an operator: the danger is not "someone can scribble on an open journal" but the ordinary unsynced-tail window that any crash already exposes, and 27278 keeps the damage to one index.

## Scenario 2 - real disk-full on Linux (loop device)

Preferred: an ext4 filesystem on a loop device. Needs `sudo` once to mount.

**Size this correctly or the run is meaningless.** The original version of this section said 512 MB with `RAVEN_24520_SAMPLE=500`, which cannot work: Voron preallocates journals and scratch, so a small dataset's index rebuild needs no new allocation and produces **no ENOSPC at all**. Three Windows runs at 40 MB / 8 MB / even 1 MB free finished with zero disk-full entries for exactly this reason - inconclusive, not passes. Use the ~130k-doc golden (`SAMPLE=50`) and a volume big enough to hold golden + work (~1.5 GB) plus headroom.

```bash
dd if=/dev/zero of=/tmp/rdb-diskfull.img bs=1M count=5120   # 5 GB, NOT 512 MB
mkfs.ext4 -q /tmp/rdb-diskfull.img
mkdir -p /tmp/rdb-diskfull
sudo mount -o loop /tmp/rdb-diskfull.img /tmp/rdb-diskfull
sudo chown "$USER" /tmp/rdb-diskfull
export RAVEN_24520_BASE=/tmp/rdb-diskfull/24520
export RAVEN_24520_SAMPLE=50                                 # ~130k docs, NOT 500
```

Sizing on a box with little spare room: the volume must hold golden (~1.2 GB) + work (~1.2 GB). `staging-dumps` costs another ~848 MB because `StageDumps` cannot hard-link across devices and falls back to `File.Copy` - **delete `$RAVEN_24520_BASE/staging-dumps` once `seed` finishes**, nothing reads it afterwards. 5 GB is then comfortable. Note `dd` may produce a **sparse** backing file (67 MB allocated for 5 GB apparent here), which is fine: the loop filesystem enforces its own size, so ENOSPC still comes from the volume under test - just make sure the *host* filesystem has more free space than the image's nominal size, or the host runs out first and you are measuring the wrong device.

### Two Linux-only harness bugs that had to be fixed before this scenario could work at all (2026-08-10)

Both made `diskfull` structurally incapable of reaching ENOSPC on Linux, and both fail in the direction that *looks* like a pass.

1. **The balloon consumed no space.** `DiskFullAsync` sized the balloon with `FileStream.SetLength`, which on Linux is `ftruncate(2)`: it sets `i_size` and allocates **zero blocks**. Measured on this ext4 - a 1 GB `SetLength` moved available space by **0 MB**, while `posix_fallocate` of the same size consumed the full **1024 MB**. The volume therefore never filled, every write succeeded, and the run reported `INCONCLUSIVE` no matter how small `leaveMB` was. NTFS allocates clusters when a file is extended, which is why the Windows runs worked. Fixed by allocating through `posix_fallocate` (falling back to writing real zeros), plus an explicit abort if free space does not actually drop - an inconclusive run must not be mistakable for a pass.
2. **The balloon and the free-space measurement targeted the wrong device.** The volume root came from `Path.GetPathRoot(Path.GetFullPath(dir))`, which on Linux is always `/` - unwritable by a normal user, so the run died in the uncaught `FileStream` throw, and free space was read from the root filesystem rather than the loop volume. Fixed with `stat -c %m` (the mount point of the path; `stat` is already a harness dependency). `diskfull` now prints the volume it is about to balloon.

Treat any pre-2026-08-10 Linux `diskfull` result as invalid - it could not have filled anything.

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

## Notes / observations

Box for the 2026-08-10 run: Linux 6.8.0-1059-azure, .NET SDK 10.0.302, 12 cores / 11 GB RAM, a single ext4 `/dev/sda1`. Branch `RavenDB-24520` at `80448c3c203` (`git merge-base --is-ancestor 5e4f97e32d5 HEAD` passes, so the 27156 fixes are in).

**Harness portability.** Both projects build clean and the harness runs unmodified. The Linux paths all work as designed: `link(2)` for hard links (17 links across 9 groups per `restore-work`), `stat -c %d:%i` for inode identity, `stat -c %h` for link counts. `FindServerDll` locates the server by walking up to `RavenDB.slnx`. `StageDumps` hard-links the source dumps when they sit on the same filesystem, so staging costs no extra space - it silently falls back to `File.Copy` (848 MB for `seed 2`) when `BASE` is on a different volume, which matters when sizing the disk-full loop image.

**One genuine Linux portability bug in the harness, in `diskfull`.** `DiskFullAsync` derives the balloon location from `Path.GetPathRoot(Path.GetFullPath(dir))`. On Linux that returns `/` regardless of the actual mount, with two consequences: the balloon is created at `/rdb24520-balloon.bin`, which a normal user cannot write, so the uncaught `FileStream` throw kills the run before anything fills; and `new DriveInfo("/").AvailableFreeSpace` measures the root filesystem instead of the loop volume, so the balloon size is computed against the wrong device. Any Linux `diskfull` run needs the volume root resolved properly first - `stat -c %m <path>` returns the mount point directly and the harness already requires `stat` on PATH.

**Nothing else diverged.** The 16-cell matrix, the post-fix suites and the AV loop all reproduce the Windows results (tables above). The ext4 hard-link cap (~65000) and case sensitivity were not exercised in any way that changed behavior: with 6 index branches the link counts stay at 7, and index dir names round-trip their exact case.

**Sync state on Linux behaves as the methodology note requires.** The golden's map shows the root `@SharedJournals` at `txId=2 lastSyncedJournal=1`, `Questions_Search` at `txId=10 lastSyncedJournal=2`, `Users_Search` at `txId=4 lastSyncedJournal=2`, and the four small indexes unsynced at `txId=1` - the same shape the Windows cells were aimed at, so cell targeting means the same thing on both platforms.

**F-9 reproduces on Linux with the identical cause**, not merely the identical outcome. Cell `1F-delete-old` fails the whole database with

```
InvalidJournalException: No such journal '/tmp/24520/work/Databases/so/Indexes/@SharedJournals/Journals/0000000000000000001.journal'.
Journal details: LastSyncedJournal - 1, LastSyncedTransactionId - 2, Flags - None
```

and, as F-9 records, no remedy is offered in the message. This branch does not carry the RavenDB-27293 diagnostics fix, so that is expected here.

**Watch the log-scoping when re-checking a single cell.** Every `cell` invocation deletes `LogsDir` on entry, so the server log explaining a failure is destroyed by the *next* cell. To establish the cause of a non-clean cell, re-run that cell alone and read `$RAVEN_24520_BASE/logs` before running anything else.
