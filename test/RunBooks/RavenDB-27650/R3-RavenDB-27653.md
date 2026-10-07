# R3 - RavenDB-27653: upgrade from 7.2 and restore of 7.2 backups

A 7.2 package creates a shared-journals database (4 indexes, 2000 documents), 8.0 (this branch, in-process) then opens it.
Code: `UpgradeFromV72` in test/Tryouts/SharedJournals24514.cs.

## Setup

1. Get the 7.2 packages (downloads need approval):
   - 7.2.7-rc-72044: https://daily-builds.s3.amazonaws.com/RavenDB-7.2.7-rc-72044-windows-x64.zip (139.8 MB),
     https://daily-builds.s3.amazonaws.com/RavenDB-7.2.7-rc-72044-linux-x64.tar.bz2 (188.8 MB)
   - 7.2.6: the official package for the OS.
2. Extract and point the tool at the server executable:
   - Windows: `$env:RAVEN_24514_V72_SERVER = '<extracted>\Server\Raven.Server.exe'`
   - Linux: `export RAVEN_24514_V72_SERVER=<extracted>/RavenDB/Server/Raven.Server`
3. Optional `RAVEN_24514_WORK` (default `D:\temp\24514`, Linux `~/ravendb-24514`). Port 8099 must be free.
4. Unset any `RAVEN_Storage_*` profile variable; the tool strips them for the 7.2 process anyway.

## Matrix (Windows and Linux)

| Row | Command |
|---|---|
| 7.2.7-rc, clean stop | `Tryouts u-swap` |
| 7.2.7-rc, kill under load | `Tryouts u-kill` |
| 7.2.7-rc, snapshot restore | `Tryouts u-restore` |
| 7.2.6, kill under load | `RAVEN_24514_V72_SERVER` set to the 7.2.6 server, `Tryouts u-kill` |

- u-swap unloads the database on 7.2 before stopping it, so 8.0 finds flushed data.
- u-kill kills 7.2 while documents are being written, so 8.0 replays unflushed 7.2 journals (the legacy reader path).
- u-restore takes a 7.2 snapshot backup and restores it into 8.0.
- Every row then deletes 1000 of the seeded documents and restarts 8.0 on the upgraded data.

Confirmed on Windows (Version in `System/headers.one`): 7.2.6 writes Voron format 24 (From24 then From25 on 8.0), 7.2.7-rc
writes 25 (From25 only). The bump went into 7.2 on 2026-09-29 (8e233d2ab51).

## Expected results

- The run prints `Upgrade from 7.2 '<variant>': PASSED.` and `DONE.`; an exception text is a FAIL.
- On the way, per open of the upgraded data:
  - every index has role Branch (shares journals again);
  - no faulty index; every map index covers exactly the Items documents; the map-reduce indexes have results;
  - Items count at least the count read before 7.2 stopped (u-kill: before the kill);
  - none of the `recyclable-journal.*` files that 7.2 left behind survives (the line `7.2 phase done: N items, M reusable
    journal files left by 7.2` shows M; with M = 0 that check had nothing to look at, say so in the result);
  - after the deletes and the restart, Items = before - 1000 and the indexes match.
