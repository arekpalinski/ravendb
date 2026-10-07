# R4 - RavenDB-27654: shared journals layouts and long paths

Retest of RavenDB-24514 (junctions / second drive), RavenDB-24524 (long paths) and RavenDB-24513 scenario 3.2.

## Layouts (both OSes)

Code: test/Tryouts/SharedJournals24514.cs. It moves Journals / Indexes / @SharedJournals folders to a second volume and links
them back (Windows: junctions, `robocopy /MOVE`; Linux: symlinks, `mv`), then checks the shared-journal role of every index
and the data. A wrong role now fails the scenario (it used to be printed only).

- Windows: reconnect E:, `$env:RAVEN_24514_VOLUME = 'E:\arek'` (default), work dir `D:\temp\24514` (or `RAVEN_24514_WORK`).
- Linux: loop-mounted ext4 as the second volume (README), `export RAVEN_24514_VOLUME=/mnt/arek`, work dir `~/ravendb-24514`.
  The tool refuses to run when both are on the same filesystem.

| Row | Command | Expected role |
|---|---|---|
| 1: whole Indexes folder on the second volume | `Tryouts 1` | all Branch |
| 2: half the index folders moved | `Tryouts 2` | moved = None, others = Branch |
| 3: only @SharedJournals on the second volume | `Tryouts 3` | all None, no faulty index |
| 4a: database Journals on the second volume | `Tryouts 4a` | all Branch |
| b1: snapshot backup and restore | `Tryouts b1` | restored indexes Branch |

Expected: every row ends with `DONE.` and no exception; integrity checks pass; after the run no journal file was renamed
across volumes (each volume's Journals folders hold only their own files) and reusable journal files
(`recyclable-journal.*`) appear in the @SharedJournals Journals folder, never in an index folder.

## Long paths (Windows)

Suspected from code (findings s-19): reusable journal paths are built without the `\\?\` prefix, so with a journal folder of
about 217-259 characters the native zeroing (CreateFileW) and reuse (MoveFileExW) fail. Both failures are caught: expected
impact is journals not being reused, no data loss.

1. Record `Get-ItemProperty HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem -Name LongPathsEnabled`.
2. Two runs, data dir chosen so that `<DataDir>\Databases\LP\Journals` is 200 characters (control) and 240 characters.
3. Server: `Raven.Server.exe --DataDir=<dir> --Logs.MinLevel=Debug --Logs.Path=<dir>\logs --ServerUrl=http://127.0.0.1:8090 --Setup.Mode=None --License.Eula.Accepted=true --Security.UnsecuredAccessAllowed=PublicNetwork`
4. Create database `LP` with one index, write about 200 MB of documents in small batches (journals must roll and get reused).
5. Look in the logs for `Failed to prepare` and `Failed to create a zeroed pool journal`.

Expected: none in the 200 run; in the 240 run either none (the prefix is not needed on this machine) or the messages above
(s-19 confirmed, file it). Data intact in both.

## RavenDB-24513 3.2: a database from a copied shared-journals folder

1. Create a database with 3 indexes, write documents, wait for indexing, delete 2 of the indexes, write a few more documents.
2. Stop the server, copy the database folder to a new path with a plain copy (hard links become separate files).
3. Start the server, create a database whose `DataDir` setting points at the copy.

Expected: it loads, the remaining index is Normal with all documents, the deleted indexes' commits in @SharedJournals are
skipped, and the log has no stream of "Failed to ensure hard link" warnings (RavenDB-26663).
