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

## Post-fix retest (do this first)

The Windows run already confirmed RavenDB-27156 + RavenDB-27166 (8/8 fix tests green; 14/14 AV-loop runs clean vs a ~40-60% pre-fix crash rate). Repeat on Linux:

- [ ] `dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~RavenDB_27156|FullyQualifiedName~RavenDB_27166"` -> expect all green.
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

Preferred: a small ext4 filesystem on a loop device. Needs `sudo` once to mount.

```bash
dd if=/dev/zero of=/tmp/rdb-diskfull.img bs=1M count=512
mkfs.ext4 -q /tmp/rdb-diskfull.img
mkdir -p /tmp/rdb-diskfull
sudo mount -o loop /tmp/rdb-diskfull.img /tmp/rdb-diskfull
sudo chown "$USER" /tmp/rdb-diskfull
export RAVEN_24520_BASE=/tmp/rdb-diskfull/24520
export RAVEN_24520_SAMPLE=500
```

Then:
- [ ] `... -- seed 2` (small golden on the loop volume, so hard links form there)
- [ ] `... -- restore-work`
- [ ] `... -- diskfull /tmp/rdb-diskfull/24520/work 40`
- [ ] Expect the Windows result: graceful `DiskFullException` (ENOSPC) -> catastrophic DB unload, **server alive**, clean recovery after space is freed. A process crash would mean F-3 regressed on Linux.
- [ ] `... -- verify /tmp/rdb-diskfull/24520/work`

Cleanup: `sudo umount /tmp/rdb-diskfull && rm /tmp/rdb-diskfull.img`, and unset the env vars.

Alternative without sudo: rely on the injected `SimulatePartialJournalWriteFailure` seam (the committed e2e) for the torn-write path; the real-volume-full path then differs only in the storage-space monitor, which can be recorded as Windows-verified.

## Notes / observations (fill in)

(Linux-specific differences vs the Windows findings go here.)
