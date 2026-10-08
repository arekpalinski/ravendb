# Voron v8 Testing - Phase III: retest after the performance improvements

Parent: RavenDB-27650. Each subtask has its own runbook here; both OSes report in the same table.

| Subtask | Runbook | Retests |
|---|---|---|
| RavenDB-27651 Shared journals: corruption and recovery | [R1-RavenDB-27651.md](R1-RavenDB-27651.md) | RavenDB-24520 |
| RavenDB-27652 Crash tests with concurrent journal writes | [R2-RavenDB-27652.md](R2-RavenDB-27652.md) | RavenDB-24528 |
| RavenDB-27653 Upgrade from 7.2 and restore of 7.2 backups | [R3-RavenDB-27653.md](R3-RavenDB-27653.md) | RavenDB-24511, 24514 (4b), 24503 |
| RavenDB-27654 Shared journals layouts and long paths | [R4-RavenDB-27654.md](R4-RavenDB-27654.md) | RavenDB-24514, 24524, 24513 (3.2) |
| RavenDB-27655 Hole punching after the idle-only change | [R5-RavenDB-27655.md](R5-RavenDB-27655.md) | RavenDB-24503 |
| RavenDB-27656 Voron stress tests, b-shared-kill, 1024 branches | [R6-RavenDB-27656.md](R6-RavenDB-27656.md) | PR stress campaign, RavenDB-24069 |

The Linux half is run by a Claude session on the Ubuntu box with [linux-prompt.md](linux-prompt.md).

## Build

Branch `RavenDB-Phase-III-retest` (v8.0 with the RavenDB-27626 and RavenDB-27644 fixes, plus the test tools).

```
dotnet build test/Tryouts/Tryouts.csproj -c Release
dotnet build test/Tryouts/Tryouts.csproj -c Debug      # only for stress scenarios marked Debug in R6
```

Every tool runs through Tryouts, picked by the first argument:

```
dotnet test/Tryouts/bin/Release/net10.0/Tryouts.dll <command> [options]
```

- RavenDB-24520 harness: `seed | status | map | journal-stats | restore-work | cell | corrupt-live | server | verify | diskfull`
- RavenDB-24528 crash tool: `node-info | scenario | carscenario | negative | integrity | numbers-seed | numbers-scenario`
- RavenDB-24514 layouts: `1 | 2 | 3 | 4a | 4b | b1 | b2 | all`, upgrade from 7.2: `u-swap | u-kill | u-restore`
- stress scenarios: `<name> --seed N --minutes M --dir <path>` (list: run Tryouts without arguments)

The server the tools start is `src/Raven.Server/bin/Release/net10.0/Raven.Server(.exe)`, override with `RAVEN_SERVER_PATH`.

## Profiles

Set as environment variables before starting a tool; external servers inherit them. Check one run with
`GET /admin/configuration/settings` (look under `ServerValues`).

| Profile | Variables | Note |
|---|---|---|
| default | none | on NVMe the device is classified Fast: journals prepared ahead of time, LZ4, 512 KB compression threshold |
| pipeline | `RAVEN_Storage_PipelineJournalWritesAboveLatencyInTicks=0` | the same knob is the classification threshold, so the device becomes Unknown: no journals prepared ahead of time, LZ4, the configured compression threshold |
| zstd | `RAVEN_Storage_JournalsCompressionAlgorithm=Zstd` plus the pipeline variable | without the pipeline variable a Fast device compresses only transactions of 512 KB or more |
| idle-1 | `RAVEN_Storage_TimeToPunchSparseRegionsAfterIdleInMin=1` | R5, Windows |

The pipeline profile does not make the server write two journal entries at once: the transaction merger submits the next
write only after the previous one is durable (findings n-6). `journal-stats` shows it: an entry with
`DurableTxIdDeltaAtSubmit >= 2` was written while an earlier one was in flight.

## Reporting

One table per OS and subtask, pasted into the ticket as a comment:

| Area | Pass/Fail | Findings / bug links |
|---|---|---|

Above the table: commit (`git log -1 --format=%h`), machine, OS and kernel, disk model, filesystem of the data dir.
A FAIL keeps its data dir; attach the tool output and the server logs, open a Bug ticket that relates to the subtask.

## Linux notes

- Data dirs on ext4, never on tmpfs: `findmnt -T /tmp` shows tmpfs on many distros, pass a `--dir` / `--data` under `~`.
- A second filesystem is a loop-mounted image: 1 GB is enough for R4's second drive; R1's disk-full run needs 5 GB
  (sizing in test/RunBooks/RavenDB-24520/20-LINUX-runbook.md, Scenario 2). Create one at a time:

```
sudo fallocate -l 1G /var/tmp/raven-vol2.img && sudo mkfs.ext4 -F -q /var/tmp/raven-vol2.img
sudo mkdir -p /mnt/arek && sudo mount -o loop /var/tmp/raven-vol2.img /mnt/arek && sudo chown "$USER:$USER" /mnt/arek
stat -c %d ~ /mnt/arek     # the two numbers must differ
```

- io_uring must be available for the IoRing rows (`--mode IoRing` fails to start otherwise).

## Low disk (about 11 GB free)

With less than about 25 GB free, run one subtask at a time, delete its data dirs once it passes, and use these settings.
Check `df -h ~` before each subtask. Sizes were measured on Windows; Linux should be close.

| What | Peak | Setting |
|---|---|---|
| Release build (Tryouts, SlowTests, FastTests, Raven.Server) | ~3.6 GB | a rebuild reuses the same folders |
| Debug build | ~3.6 GB more | build it only before R6 |
| First NuGet restore | a few GB | only on a box that never built the repo |
| R1 | ~3.2 GB, plus the 5 GB disk-full image | copy only `posts-001.dump`, `posts-002.dump`, `users.dump` (0.83 GB) and `SO-indexes.ravendbdump`, then `RAVEN_24520_DUMPS=<that folder>` and `RAVEN_24520_INDEXES=<folder>/SO-indexes.ravendbdump`; delete `$RAVEN_24520_BASE/staging-dumps` after `seed`; run the encrypted variant after deleting the plain golden and work dirs |
| R2 | ~1-2 GB | the same `--data` for every row (each run wipes it) and `--seed-count 100000 --workers 4 --load-seconds 10`; the default load made 6.7 GB in a 20-second run |
| R3 | ~1 GB | delete each 7.2 tarball once extracted |
| R4 | ~1.5 GB | the 1 GB loop image |
| R5 | ~2 GB | Linux row only, import just the three dumps above |
| R6 | small, except d-drain-hammer | iteration dirs are deleted on PASS; skip d-drain-hammer (it grows a data file at disk speed, about 30 GB per minute of budget); run d-avalanche with `--minutes 1` or skip it (a measurement) |
- Encrypted databases need a server built with `-p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP` and a raised memlock limit.
