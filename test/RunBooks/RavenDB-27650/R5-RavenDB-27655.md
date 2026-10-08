# R5 - RavenDB-27655: hole punching after the idle-only change

Retest of the RavenDB-24503 restart scenarios. No tool, a server and a stopwatch.

## Why the expectations changed

- Windows (`Storage.PunchSparseRegionsOnIdleOnly` is true by default there): regions freed by a flush are kept in memory and
  punched only after `Storage.TimeToPunchSparseRegionsAfterIdleInMin` (5) minutes without writes, about one region every
  5 seconds of idle time. A restart drops that list; the scan at database load finds the free ranges again and the clock
  starts over.
- Linux punches at sync time as before, but syncs are postponed now (up to 256 MB unsynced).
- CI never runs the Windows path: test defaults turn idle-only punching off.

## Steps

1. Start a Release server with the data dir on the disk to measure (`--Logs.MinLevel=Info`).
2. Create database `HP`, import StackOverflow dumps from `D:\workspace\stackoverflow-data` (several GB; Studio import or smuggler).
3. Delete about half of the documents in large contiguous ranges (DeleteByQuery on id ranges), then stop writing.
4. Read the data file's size from the storage report (Studio: Stats > Storage Report), every minute.

| Row | Profile | What to measure |
|---|---|---|
| Windows default | none | physical size before and after; minutes from the last write until it stops shrinking |
| Windows idle-1 | `RAVEN_Storage_TimeToPunchSparseRegionsAfterIdleInMin=1` | same |
| Windows restart inside the idle window | none | restart 2 minutes after the last write; the clock starts over after the load |
| Linux default | none | same as the first row |

## Expected results

- The physical size drops by about the freed space, as in RavenDB-24503 (there: about 98% of the free space returned).
- Windows: nothing is punched during the first 5 minutes (1 minute with idle-1); after that the size shrinks step by step.
- The restart row: punching starts 5 idle minutes after the database loaded again.
- Linux: the space comes back after the next syncs.
- No data loss: document counts and index entries unchanged across restarts.
- Compare the space that came back with the free pages (`GET /databases/HP/debug/storage/environment/free-space-snapshot`,
  FreeSpaceSize), not with the storage report's FreeSpaceInBytes: that one also counts the never-used end of the data file
  (allocated when the file grew, never punched by design). On Windows that end was 0.98 GB of an 8 GB file.
- Known bug RavenDB-27662: pages written and freed within one flush are punched only after the database loads again. Once
  the size stops shrinking, restart the database once without writes (`POST /databases/HP/admin/restart`) and record how much
  more comes back 5 minutes after the load (Windows: 0.67 GB). Repro test: `dotnet test test/SlowTests/SlowTests.csproj -c Release
  --filter "FullyQualifiedName~SparseRegionsFreedInSameFlush"` - today the `flushBetween: False` cases fail, on Linux too if the
  bug is there.

Open hole-punching tickets (RavenDB-24497, 24500, 24502, 26872) need the idle delay in their Windows expectations.
