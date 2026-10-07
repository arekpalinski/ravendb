# R2 - RavenDB-27652: crash tests with concurrent journal writes

Retest of RavenDB-24528 Part 1 with the RavenDB-24528 crash tool (`numbers-scenario`). The tool starts a real
Raven.Server as a child process, seeds the Numbers and units data, writes under load, hard-kills the server, restarts it
and checks recovery. Older notes: test/RunBooks/RavenDB-24528.

## What changed since Phase I

- The pass condition was too weak: it compared the recovered count with the count from before the load. It now requires
  `recovered >= beforeKill` - the count read right before the kill. Reads only see durable transactions, so every one of
  those documents must survive.
- Right after each kill the tool prints `[stats]` lines for the database journals (from `journal-stats`): transactions per
  journal, invalid hashes, and how many were submitted while an earlier write was in flight (`DurableTxIdDeltaAtSubmit >= 2`).
- New options: `--workers`, `--batch` (documents per save), `--reload`, `--encrypt`, `--indexes`.

## Setup

- Release build of the branch; the tool finds `src/Raven.Server/bin/Release/net10.0`.
- The index dump: Windows default `D:\workspace\ravendb-dumps\Numbers_and_units-Indexes.ravendbdump`; on Linux copy it and pass `--indexes <path>`.
- Data dir: default `D:\temp\ravendb-24528\numbers-<mode>-def` on Windows, `/tmp/ravendb-24528/...` on Linux - pass `--data` under `~` if /tmp is tmpfs.
- Port 8080 must be free.
- Low on disk: the README's "Low disk" settings (one `--data` for all rows, smaller seed and load).

`Tryouts` below means `dotnet test/Tryouts/bin/Release/net10.0/Tryouts.dll`:

```
Tryouts numbers-scenario --mode <M> --iterations 3 --load-seconds 20 --seed-count 1000000 [--data <dir>] [--indexes <dump>]
```

## Matrix (Windows and Linux)

| Row | Command | Profile |
|---|---|---|
| WriteMode Auto, FileIo, Mmap, IoRing | `numbers-scenario --mode <M> --iterations 3 --load-seconds 20 --seed-count 1000000` | default |
| VectoredFileIo (Linux only) | same, `--mode VectoredFileIo` | default |
| pipeline | `--mode Auto --iterations 3 --workers 16 --batch 1` | `RAVEN_Storage_PipelineJournalWritesAboveLatencyInTicks=0` |
| zstd | `--mode Auto --iterations 3` | pipeline variable plus `RAVEN_Storage_JournalsCompressionAlgorithm=Zstd` |
| encryption | `--mode IoRing --iterations 3 --encrypt` (Linux: `--mode Auto`) | default; server built with `-p:RAVEN_BuildOptions=ALLOW_ENCRYPTED_OVER_HTTP` |
| kill timing | `--mode Auto --iterations 1 --load-seconds <3, 7, 12, 20, 30>` (five runs) | default |
| reload under load | `--mode Auto --iterations 3 --reload --workers 4 --batch 1` | default |

Unset the profile variables after their rows.

## Expected results

- Every iteration prints `-> PASS`, and the summary line is `N passed, 0 failed`.
- `recovered=...` is at least `beforeKill`. `COUNT LOSS` in a note is a lost acknowledged write: stop, keep the data dir, file a bug.
- No index errors, no ERROR/FATAL lines in the recovery logs, `selected WriteMode` is what was asked (Auto picks IoRing on Windows).
- `[stats]`: every journal has a header record and 0 invalid hashes.
- The pipeline row: record the "submitted while an earlier one was in flight" total. On Windows NVMe it was 0 for every load
  tried (findings n-6: the transaction merger submits the next write only after the previous one is durable). A non-zero
  number is not a failure, it is worth a note.
- Reload rows: `reload: X before the unload, Y after` with Y >= X, no `.unrecovered` file. On Linux this is the path where
  RavenDB-27626 lost acknowledged writes.

Not in this subtask: IoRingQueueSize validation and config precedence (untouched; RavenDB-26859/26860 have CI tests).
