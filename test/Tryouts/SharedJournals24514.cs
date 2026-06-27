using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace Tryouts;

// RavenDB-24514 - Shared Journals: junction points / different drives.
// Drives the v8 server through layouts where Journals / Indexes / @SharedJournals are relocated to a
// second physical volume (E:) via Windows directory junctions, then records the per-index shared-journal
// mode (Root/Branch/None) and hard-asserts data integrity. See the plan / shared-journals.md for the
// gate asymmetry being validated (the probe compares index-journal-path vs the *database* journal path,
// not vs @SharedJournals).
// ---- LINUX PORTING NOTES (Hyper-V VM, single disk) -------------------------------------------------------
// EXDEV (the cross-device hard-link failure this harness exercises) triggers across different *filesystems*,
// not different directories. On a single-disk VM, make a second filesystem with a loopback image:
//   sudo fallocate -l 4G /var/tmp/raven-vol2.img && sudo mkfs.ext4 -F -q /var/tmp/raven-vol2.img
//   sudo mkdir -p /mnt/arek && sudo mount -o loop /var/tmp/raven-vol2.img /mnt/arek && sudo chown "$USER:$USER" /mnt/arek
//   verify: stat -c %d /var/tmp vs /mnt/arek (must differ); ln /var/tmp/x /mnt/arek/x -> "Invalid cross-device link"
// Then run with RAVEN_24514_VOLUME=/mnt/arek and a Linux WorkRoot. Avoid tmpfs (no O_DIRECT -> Voron noise)
// and bind mounts (same st_dev -> hard links succeed, won't reproduce).
//
// The filesystem helpers below are Windows-only; port them for Linux:
//   CreateJunction (mklink /J)            -> ln -s target link
//   RelocateViaJunction (robocopy /MOVE)  -> mv (copies+unlinks across fs) then ln -s
//   RemoveJunction* (rmdir/ReparsePoint)  -> File.Delete on the symlink; detect via File.ResolveLinkTarget
//   EnsureSecondVolume                    -> BUG on Linux: Path.GetPathRoot is "/" for all paths, so it
//                                            wrongly aborts. Compare st_dev instead (e.g. stat -c %d).
//   WorkRoot const (D:\temp\24514)        -> Linux path, or make it env-overridable like SecondVolumeRoot
//   StartV72Server V72Repo                -> Linux 7.2 clone path (dotnet run + -n/--no-launch-profile/Local carry over)
// ----------------------------------------------------------------------------------------------------------
public class SharedJournals24514 : RavenTestBase
{
    public SharedJournals24514(ITestOutputHelper output) : base(output)
    {
    }

    private static readonly string SecondVolumeRoot = Environment.GetEnvironmentVariable("RAVEN_24514_VOLUME") ?? @"E:\arek";   // real external drive; override via env var for a different second volume
    private const string WorkRoot = @"D:\temp\24514";     // data dir stays on D:; only Journals/Indexes move to E:
    private const string V72Repo = @"D:\workspace\ravendb-7.2";
    private const int DocCount = 2000;

    private readonly List<string> _junctions = new();     // junction links created this run, removed on cleanup
    private string _runId;
    private int _relocCounter;

    // ---- entry points -------------------------------------------------------

    public async Task RunAll()
    {
        await Scenario1_AllIndexesOnSecondDrive();
        await Scenario2_SomeIndexesOnSecondDrive();
        await Scenario3_SharedJournalsOnSecondDrive();
        await Scenario4a_DatabaseJournalsOnSecondDrive();
        // 4b (true v7.2 -> v8 upgrade) is opt-in: it launches an external server and depends on the 7.2 build.
    }

    // ---- scenarios ----------------------------------------------------------

    // 1: whole Indexes/ (incl. @SharedJournals) on E:. Hypothesis: probe index(E:) vs db-journal(D:) -> all None.
    public Task Scenario1_AllIndexesOnSecondDrive() =>
        RunInProcScenario(1, "all indexes on E: (relocate whole Indexes/)",
            relocate: paths => new List<string> { RelocateViaJunction(paths.IndexesRoot) },
            verify: (db, roles) => ReportRoles(roles, expectAllIndexes: null /* observe: idx+@SJ both on E:, but links were broken by the move */));

    // 2: half the index folders on E:, half on D:. Hypothesis: relocated -> None, others -> Branch.
    public Task Scenario2_SomeIndexesOnSecondDrive()
    {
        List<string> relocatedNames = new();
        return RunInProcScenario(2, "some indexes on E: (relocate half the index folders)",
            relocate: paths =>
            {
                List<(string Name, string Base)> half = paths.IndexEnvs.Take(Math.Max(1, paths.IndexEnvs.Count / 2)).ToList();
                List<string> links = new();
                foreach ((string name, string baseDir) in half)
                {
                    relocatedNames.Add(name);
                    links.Add(RelocateViaJunction(baseDir));
                }
                return links;
            },
            verify: (db, roles) => ReportRolesPerIndex(roles, relocatedToE: relocatedNames));
    }

    // 3: only Indexes/@SharedJournals on E:. Highest risk - gate passes (index D: vs db D:) but the runtime
    // link index(D:) -> @SharedJournals(E:) must fail. Record whether it falls back gracefully or errors.
    [RavenFact(RavenTestCategory.Indexes)]
    public Task Scenario3_SharedJournalsOnSecondDrive() =>
        RunInProcScenario(3, "only @SharedJournals on E:",
            relocate: paths => new List<string> { RelocateViaJunction(paths.SharedJournals) },
            verify: (db, roles) => ReportRoles(roles, expectAllIndexes: "None" /* fixed gate: cross-volume @SJ -> unshared, no fault */));

    // 4a: the headline case (fresh v8). DB's own Journals/ on a fast drive (E:), data+indexes on D:.
    // Hypothesis: probe db-journal(E:) vs index(D:) -> false -> every index None though index<->@SharedJournals (both D:) would link.
    public Task Scenario4a_DatabaseJournalsOnSecondDrive() =>
        RunInProcScenario(4, "DB journals on a fast drive (E:), fresh v8",
            relocate: paths => new List<string> { RelocateViaJunction(paths.DbJournals) },
            verify: (db, roles) => ReportRoles(roles, expectAllIndexes: "Branch" /* fixed gate: idx+@SJ both on D: -> sharing restored */));

    // 4b: true v7.2 -> v8 upgrade with journals on a fast drive. External v7.2 process creates the data;
    // v8 opens it. Opt-in (needs the 7.2 build). Best-effort: reports clearly if 7.2 can't be launched.
    public async Task Scenario4b_UpgradeFromV72()
    {
        EnsureSecondVolume();
        _runId = $"s4b-{Guid.NewGuid():N}";
        string dataDir = Path.Combine(WorkRoot, "s4b");
        string dbName = "SJ_24514_s4b";
        string url = "http://127.0.0.1:8099";
        PrepareCleanDir(dataDir);

        Process v72 = null;
        try
        {
            Output.WriteLine($"\n########## Scenario 4b: true v7.2 -> v8 upgrade, journals on fast drive ##########");
            v72 = StartV72Server(dataDir, url);
            await WaitForServerReady(url, TimeSpan.FromMinutes(3));

            // populate via v8 client against the v7.2 server (basic CRUD/index ops are cross-version safe)
            using (IDocumentStore store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize())
            {
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(dbName)));
                await CreateIndexes(store);
                await Seed(store, dbName);
                Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2));
            }

            StopServer(v72);
            v72 = null;

            // the documented 7.x "journals on a fast drive" setup: junction the DB's Journals folder to E:
            string dbJournals = Path.Combine(dataDir, "Databases", dbName, "Journals");
            if (Directory.Exists(dbJournals) == false)
                throw new InvalidOperationException($"expected v7.2 to have created '{dbJournals}'");
            RelocateViaJunction(dbJournals);
        }
        catch (Exception e)
        {
            if (v72 != null)
                StopServer(v72);
            Output.WriteLine($"Scenario 4b could not run the v7.2 phase: {e.Message}");
            Output.WriteLine("Check the v7.2 launch command (StartV72Server) and that the 7.2 build is restored. Skipping.");
            Cleanup(dataDir);
            return;
        }

        // upgrade: open the same data dir with the in-process v8 server
        try
        {
            using RavenServer server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                DataDirectory = dataDir,
                DeletePrevious = false,
                RegisterForDisposal = true,
                CustomSettings = BaseSettings(dataDir, url)
            });
            using IDocumentStore store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();
            Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2), allowErrors: true);
            DocumentDatabase db = await Databases.GetDocumentDatabaseInstanceFor(server, store);
            DumpSharedJournalState(db, "after upgrade to v8 (DB journals on a 2nd volume)");
            ReportRoles(GetRoles(db), expectAllIndexes: "Branch" /* fixed gate: index + @SharedJournals co-located -> sharing kept across the upgrade */);
            await AssertIntegrity(store, dbName);
        }
        finally
        {
            Cleanup(dataDir);
        }
    }

    // ---- backup / restore (scenario B) --------------------------------------

    // B1: snapshot backup of a shared-journals DB, restore under a new name, verify the restored indexes
    // re-share (Branch) and data is intact. Snapshot excludes @SharedJournals and reconstructs it on restore.
    public async Task ScenarioB1_SnapshotBackupRestore()
    {
        string backupPath = NewDataPath(suffix: "24514-b1-backup");
        using IDocumentStore store = GetDocumentStore(new Options { RunInMemory = false });
        string dbName = store.Database;

        Output.WriteLine("\n########## Scenario B1: snapshot backup + restore (shared journals) ##########");
        await CreateIndexes(store);
        await Seed(store, dbName);
        Indexes.WaitForIndexing(store, dbName);

        DocumentDatabase db = await GetDatabase(dbName);
        DumpSharedJournalState(db, "source (before backup)");
        DatabaseStatistics before = await store.Maintenance.SendAsync(new GetStatisticsOperation());

        PeriodicBackupConfiguration config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
        await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
        string backupLocation = Directory.GetDirectories(backupPath).First();
        Output.WriteLine($"snapshot created at: {backupLocation}");

        string restoredName = dbName + "_restored";
        using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { DatabaseName = restoredName, BackupLocation = backupLocation }))
        {
            DocumentDatabase restored = await GetDatabase(restoredName);
            DumpSharedJournalState(restored, "restored");
            ReportRoles(GetRoles(restored), expectAllIndexes: "Branch");

            DatabaseStatistics after = await store.Maintenance.ForDatabase(restoredName).SendAsync(new GetStatisticsOperation());
            Assert.Equal(before.CountOfDocuments, after.CountOfDocuments);
            Assert.Equal(before.CountOfIndexes, after.CountOfIndexes);

            await AssertIntegrity(store, restoredName);
            Output.WriteLine("Scenario B1: backup/restore integrity PASSED.");
        }
    }

    // B2: snapshot-backup a DB whose journals are relocated to the 2nd volume (the documented "journals on a
    // fast drive" layout), then restore to a normal layout. Confirms backup reads through the junction and the
    // restored DB is intact and re-shares.
    public async Task ScenarioB2_BackupFromRelocatedJournals()
    {
        EnsureSecondVolume();
        _runId = $"b2-{Guid.NewGuid():N}";
        string dataDir = Path.Combine(WorkRoot, "b2");
        string backupPath = Path.Combine(WorkRoot, "b2-backup");
        string restoreDir = Path.Combine(WorkRoot, "b2-restore");
        string dbName = "SJ_24514_b2";
        string url = "http://127.0.0.1:8095";
        PrepareCleanDir(dataDir);
        PrepareCleanDir(backupPath);
        PrepareCleanDir(restoreDir);

        try
        {
            Output.WriteLine("\n########## Scenario B2: snapshot backup of journals-on-fast-drive DB + restore ##########");
            DatabaseStatistics before;
            DbPaths paths;

            using (RavenServer server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false, DataDirectory = dataDir, DeletePrevious = true, RegisterForDisposal = true, CustomSettings = BaseSettings(dataDir, url)
            }))
            {
                using IDocumentStore store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(dbName)));
                await CreateIndexes(store);
                await Seed(store, dbName);
                Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2));
                DocumentDatabase db = await Databases.GetDocumentDatabaseInstanceFor(server, store);
                paths = CapturePaths(db);
                before = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            }

            RelocateViaJunction(paths.DbJournals); // DB journals -> 2nd volume

            using (RavenServer server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false, DataDirectory = dataDir, DeletePrevious = false, RegisterForDisposal = true, CustomSettings = BaseSettings(dataDir, url)
            }))
            {
                using IDocumentStore store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();
                Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2), allowErrors: true);
                DocumentDatabase db = await Databases.GetDocumentDatabaseInstanceFor(server, store);
                DumpSharedJournalState(db, "source (journals on 2nd volume)");

                PeriodicBackupConfiguration config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                await Backup.UpdateConfigAndRunBackupAsync(server, config, store);
                string backupLocation = Directory.GetDirectories(backupPath).First();
                Output.WriteLine($"snapshot created at: {backupLocation}");

                string restoredName = dbName + "_restored";
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { DatabaseName = restoredName, BackupLocation = backupLocation, DataDirectory = restoreDir }))
                {
                    DocumentDatabase restored = await Databases.GetDocumentDatabaseInstanceFor(server, store, restoredName);
                    DumpSharedJournalState(restored, "restored (normal layout)");
                    ReportRoles(GetRoles(restored), expectAllIndexes: "Branch");
                    await AssertIntegrity(store, restoredName);
                    DatabaseStatistics after = await store.Maintenance.ForDatabase(restoredName).SendAsync(new GetStatisticsOperation());
                    Assert.Equal(before.CountOfDocuments, after.CountOfDocuments);
                    Output.WriteLine("Scenario B2: backup-from-relocated-journals + restore integrity PASSED.");
                }
            }
        }
        finally
        {
            Cleanup(dataDir);
            foreach (string d in new[] { backupPath, restoreDir })
                try { if (Directory.Exists(d)) Directory.Delete(d, recursive: true); } catch (Exception e) { Output.WriteLine($"cleanup warning: {e.Message}"); }
        }
    }

    // ---- in-process scenario driver -----------------------------------------

    private sealed record DbPaths(string DbBase, string DbJournals, string IndexesRoot, string SharedJournals, List<(string Name, string Base)> IndexEnvs);

    private async Task RunInProcScenario(int n, string title, Func<DbPaths, List<string>> relocate, Action<DocumentDatabase, List<EnvRole>> verify)
    {
        EnsureSecondVolume();
        _runId = $"s{n}-{Guid.NewGuid():N}";
        string dataDir = Path.Combine(WorkRoot, $"s{n}");
        string dbName = $"SJ_24514_s{n}";
        string url = $"http://127.0.0.1:{8090 + n}";
        PrepareCleanDir(dataDir);

        try
        {
            Output.WriteLine($"\n########## Scenario {n}: {title} ##########");

            DbPaths paths;

            // phase 1: create + populate, capture real on-disk paths, then stop the server
            using (RavenServer server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                DataDirectory = dataDir,
                DeletePrevious = true,
                RegisterForDisposal = true,
                CustomSettings = BaseSettings(dataDir, url)
            }))
            {
                using IDocumentStore store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(dbName)));
                await CreateIndexes(store);
                await Seed(store, dbName);
                Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2));

                DocumentDatabase db = await Databases.GetDocumentDatabaseInstanceFor(server, store);
                DumpSharedJournalState(db, "baseline (before relocate)");
                paths = CapturePaths(db);
            }

            // relocate while the server is stopped (files unlocked)
            List<string> links = relocate(paths);
            Output.WriteLine($"relocated to E: via junctions: {string.Join(", ", links)}");

            // phase 2: reopen the same data dir
            using (RavenServer server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false,
                DataDirectory = dataDir,
                DeletePrevious = false,
                RegisterForDisposal = true,
                CustomSettings = BaseSettings(dataDir, url)
            }))
            {
                using IDocumentStore store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();
                Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2), allowErrors: true);
                DocumentDatabase db = await Databases.GetDocumentDatabaseInstanceFor(server, store);
                DumpSharedJournalState(db, "after relocate + restart");
                verify(db, GetRoles(db));
                await AssertIntegrity(store, dbName);
            }

            Output.WriteLine($"Scenario {n}: data integrity PASSED.");
        }
        finally
        {
            Cleanup(dataDir);
        }
    }

    private static DbPaths CapturePaths(DocumentDatabase db)
    {
        string dbBase = db.DocumentsStorage.Environment.Options.BasePath.FullPath;
        string dbJournals = db.DocumentsStorage.Environment.Options.JournalPath.FullPath;
        string sharedJournals = db.Configuration.Indexing.SharedJournalsPath.FullPath;
        string indexesRoot = Path.GetDirectoryName(sharedJournals);
        List<(string, string)> indexEnvs = db
            .GetAllStoragesEnvironment(new List<StorageEnvironmentWithType.StorageEnvironmentType> { StorageEnvironmentWithType.StorageEnvironmentType.Index })
            .Select(e => (e.Name, e.Environment.Options.BasePath.FullPath))
            .ToList();
        return new DbPaths(dbBase, dbJournals, indexesRoot, sharedJournals, indexEnvs);
    }

    // ---- verification -------------------------------------------------------

    private sealed record EnvRole(StorageEnvironmentWithType.StorageEnvironmentType Type, string Name, string Role, int Journals, int HardLinked, string JournalPath);

    private static List<EnvRole> GetRoles(DocumentDatabase db)
    {
        List<EnvRole> roles = new();
        foreach (StorageEnvironmentWithType e in db.GetAllStoragesEnvironment())
        {
            StorageEnvironmentOptions o = e.Environment.Options;
            string role = e.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals
                ? "Root"
                : o.RootJournal != null ? "Branch" : "None";
            int total = e.Environment.Journal.Files.Count;
            int hard = e.Environment.Journal.Files.Count(f => f.IsHardLinked);
            roles.Add(new EnvRole(e.Type, e.Name, role, total, hard, o.JournalPath?.FullPath));
        }
        return roles;
    }

    private void DumpSharedJournalState(DocumentDatabase db, string label)
    {
        Output.WriteLine($"--- shared-journal state [{label}] ---");
        Output.WriteLine($"    IndexStore.SharedJournals present: {db.IndexStore.SharedJournals != null}");
        foreach (EnvRole r in GetRoles(db))
            Output.WriteLine($"    {r.Type,-13} {Trim(r.Name, 34),-34} role={r.Role,-6} journals={r.Journals} hardlinked={r.HardLinked}  {r.JournalPath}");
    }

    // expectAllIndexes: "Branch"/"None" to compare every index env against, or null to just observe.
    private void ReportRoles(List<EnvRole> roles, string expectAllIndexes)
    {
        List<EnvRole> indexes = roles.Where(r => r.Type == StorageEnvironmentWithType.StorageEnvironmentType.Index).ToList();
        Output.WriteLine($"    indexes: {indexes.Count}, Branch={indexes.Count(r => r.Role == "Branch")}, None={indexes.Count(r => r.Role == "None")}");
        if (expectAllIndexes == null)
        {
            Output.WriteLine("    (observation only - no role expectation asserted)");
            return;
        }
        List<EnvRole> mismatch = indexes.Where(r => r.Role != expectAllIndexes).ToList();
        if (mismatch.Count == 0)
            Output.WriteLine($"    role expectation MATCH: all indexes are '{expectAllIndexes}' as hypothesized.");
        else
            Output.WriteLine($"    role expectation MISMATCH: expected all '{expectAllIndexes}', but: {string.Join(", ", mismatch.Select(m => $"{Trim(m.Name, 24)}={m.Role}"))}");
    }

    private void ReportRolesPerIndex(List<EnvRole> roles, List<string> relocatedToE)
    {
        foreach (EnvRole r in roles.Where(r => r.Type == StorageEnvironmentWithType.StorageEnvironmentType.Index))
        {
            string expected = relocatedToE.Contains(r.Name) ? "None" : "Branch";
            string verdict = r.Role == expected ? "MATCH" : "MISMATCH";
            Output.WriteLine($"    index {Trim(r.Name, 30),-30} expected={expected,-6} actual={r.Role,-6} {verdict}");
        }
    }

    private async Task AssertIntegrity(IDocumentStore store, string dbName)
    {
        Indexes.WaitForIndexing(store, dbName, TimeSpan.FromMinutes(2), allowErrors: true);

        DatabaseStatistics stats = await store.Maintenance.ForDatabase(dbName).SendAsync(new GetStatisticsOperation());
        List<IndexInformation> faulty = stats.Indexes.Where(i => i.State == IndexState.Error || i.Type == IndexType.Faulty).ToList();
        if (faulty.Count > 0)
        {
            IndexErrors[] allErrors = await store.Maintenance.ForDatabase(dbName).SendAsync(new GetIndexErrorsOperation());
            foreach (IndexErrors ie in allErrors.Where(x => faulty.Any(f => f.Name == x.Name)))
                foreach (IndexingError err in ie.Errors)
                    Output.WriteLine($"    FAULT [{ie.Name}] {err.Action}: {err.Error}");
        }
        Assert.True(faulty.Count == 0, $"faulty/errored indexes after relocate: {string.Join(", ", faulty.Select(f => $"{f.Name}({f.State}/{f.Type})"))}");
        Assert.True(stats.CountOfDocuments >= DocCount, $"expected >= {DocCount} docs, got {stats.CountOfDocuments}");

        using IAsyncDocumentSession session = store.OpenAsyncSession(dbName);
        foreach ((string name, bool mapReduce) in IndexNames)
        {
            long total = await CountIndexResults(session, name);
            if (mapReduce)
                Assert.True(total > 0, $"map-reduce index '{name}' returned no results");
            else
                Assert.True(total == DocCount, $"map index '{name}' returned {total}, expected {DocCount}");
        }
    }

    private static async Task<long> CountIndexResults(IAsyncDocumentSession session, string indexName)
    {
        IAsyncRawDocumentQuery<dynamic> q = session.Advanced.AsyncRawQuery<dynamic>($"from index '{indexName}' limit 0");
        q.Statistics(out QueryStatistics qs);
        await q.ToListAsync();
        return qs.TotalResults;
    }

    // ---- data ---------------------------------------------------------------

    private static readonly (string Name, bool MapReduce)[] IndexNames =
    {
        ("Items/ByName", false),
        ("Items/ByValue", false),
        ("Items/CountByName", true),
        ("Items/SumByBucket", true),
    };

    private static async Task CreateIndexes(IDocumentStore store)
    {
        await new Items_ByName().ExecuteAsync(store);
        await new Items_ByValue().ExecuteAsync(store);
        await new Items_CountByName().ExecuteAsync(store);
        await new Items_SumByBucket().ExecuteAsync(store);
    }

    private static async Task Seed(IDocumentStore store, string dbName)
    {
        using BulkInsertOperation bulk = store.BulkInsert(dbName);
        for (int i = 0; i < DocCount; i++)
            await bulk.StoreAsync(new Item { Name = $"name-{i % 50}", Value = i });
    }

    private sealed class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private sealed class Items_ByName : AbstractIndexCreationTask<Item>
    {
        public Items_ByName() => Map = items => from i in items select new { i.Name };
    }

    private sealed class Items_ByValue : AbstractIndexCreationTask<Item>
    {
        public Items_ByValue() => Map = items => from i in items select new { i.Value };
    }

    private sealed class Items_CountByName : AbstractIndexCreationTask<Item, Items_CountByName.Result>
    {
        public sealed class Result
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public Items_CountByName()
        {
            Map = items => from i in items select new Result { Name = i.Name, Count = 1 };
            Reduce = results => from r in results group r by r.Name into g select new Result { Name = g.Key, Count = g.Sum(x => x.Count) };
        }
    }

    private sealed class Items_SumByBucket : AbstractIndexCreationTask<Item, Items_SumByBucket.Result>
    {
        public sealed class Result
        {
            public int Bucket { get; set; }
            public int Count { get; set; }
        }

        public Items_SumByBucket()
        {
            Map = items => from i in items select new Result { Bucket = i.Value % 100, Count = 1 };
            Reduce = results => from r in results group r by r.Bucket into g select new Result { Bucket = g.Key, Count = g.Sum(x => x.Count) };
        }
    }

    // ---- junction / filesystem helpers --------------------------------------

    // Move an existing folder (created by the server) to a fresh dir on E: and junction it back in place.
    // Returns the junction link path (== dir).
    private string RelocateViaJunction(string dir)
    {
        if (Directory.Exists(dir) == false)
            throw new InvalidOperationException($"cannot relocate '{dir}' - it does not exist");

        string leaf = Path.GetFileName(dir.TrimEnd(Path.DirectorySeparatorChar));
        string target = Path.Combine(SecondVolumeRoot, _runId, $"{leaf}-{_relocCounter++}");
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);

        (int code, string _, string err) = RunProcess("robocopy", $"\"{dir}\" \"{target}\" /E /MOVE /NFL /NDL /NJH /NJS /NP");
        if (code >= 8) // robocopy: exit < 8 == success
            throw new InvalidOperationException($"robocopy '{dir}' -> '{target}' failed (exit {code}): {err}");

        if (Directory.Exists(dir)) // robocopy /MOVE may leave the empty source root behind; dir is on D:, no junction yet
            Directory.Delete(dir, recursive: true);

        CreateJunction(dir, target);
        return dir;
    }

    private void CreateJunction(string link, string target)
    {
        Directory.CreateDirectory(target);
        (int code, string _, string err) = RunProcess("cmd.exe", $"/c mklink /J \"{link}\" \"{target}\"");
        if (code != 0)
            throw new InvalidOperationException($"mklink /J \"{link}\" \"{target}\" failed (exit {code}): {err}");
        _junctions.Add(link);
    }

    private static bool IsJunction(string path) =>
        Directory.Exists(path) && (new DirectoryInfo(path).Attributes & FileAttributes.ReparsePoint) != 0;

    // rmdir removes the reparse point only, never the target's contents.
    private static void RemoveJunction(string link)
    {
        if (IsJunction(link))
            RunProcess("cmd.exe", $"/c rmdir \"{link}\"");
    }

    private void PrepareCleanDir(string dataDir)
    {
        RemoveJunctionsUnder(dataDir);
        if (Directory.Exists(dataDir))
            Directory.Delete(dataDir, recursive: true);
        Directory.CreateDirectory(dataDir);
    }

    private void Cleanup(string dataDir)
    {
        foreach (string link in _junctions)
            RemoveJunction(link);
        _junctions.Clear();

        RemoveJunctionsUnder(dataDir); // catch any junction not tracked (e.g. partial run)
        try
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
            string eRun = Path.Combine(SecondVolumeRoot, _runId ?? "");
            if (string.IsNullOrEmpty(_runId) == false && Directory.Exists(eRun))
                Directory.Delete(eRun, recursive: true);
        }
        catch (Exception e)
        {
            Output.WriteLine($"cleanup warning: {e.Message}");
        }
    }

    // Walk WITHOUT descending into reparse points, removing every junction found. Avoids following a
    // junction into E: and deleting real data there.
    private static void RemoveJunctionsUnder(string root)
    {
        if (Directory.Exists(root) == false)
            return;
        foreach (string sub in Directory.GetDirectories(root))
        {
            if (IsJunction(sub))
                RunProcess("cmd.exe", $"/c rmdir \"{sub}\"");
            else
                RemoveJunctionsUnder(sub);
        }
    }

    private void EnsureSecondVolume()
    {
        if (Directory.Exists(SecondVolumeRoot) == false)
            throw new InvalidOperationException(
                $"Second volume '{SecondVolumeRoot}' not found. Mount the external drive and create the folder before running RavenDB-24514 scenarios.");
        if (string.Equals(Path.GetPathRoot(SecondVolumeRoot), Path.GetPathRoot(WorkRoot), StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"'{SecondVolumeRoot}' and '{WorkRoot}' resolve to the same volume - cross-drive scenarios require two distinct volumes.");
    }

    private static IDictionary<string, string> BaseSettings(string dataDir, string url) => new Dictionary<string, string>
    {
        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url,
        ["Logs.Path"] = Path.Combine(dataDir, "logs"),
        ["Logs.MinLevel"] = "Info"
    };

    private static string Trim(string s, int max) => s == null ? "" : s.Length <= max ? s : s.Substring(0, max - 1) + "~";

    // ---- v7.2 external server (scenario 4b) ---------------------------------

    private Process StartV72Server(string dataDir, string url)
    {
        string projectDir = Path.Combine(V72Repo, "src", "Raven.Server");
        if (Directory.Exists(projectDir) == false)
            throw new InvalidOperationException($"v7.2 server project not found at '{projectDir}'");

        // --no-launch-profile so the v7.2 dev launchSettings doesn't inject its own ServerUrl/security args.
        // -n runs non-interactive (no CLI prompt). Config settings are passed as --Key=Value after the '--'.
        string args =
            $"run -c Release --no-launch-profile --project \"{projectDir}\" -- -n " +
            $"--ServerUrl={url} --DataDir=\"{dataDir}\" --Setup.Mode=None " +
            $"--Security.UnsecuredAccessAllowed=Local --License.Eula.Accepted=true";

        ProcessStartInfo psi = new("dotnet", args)
        {
            UseShellExecute = false,
            WorkingDirectory = V72Repo,
            RedirectStandardInput = true // keep stdin open so the server doesn't hit EOF and shut down right after start
        };
        Output.WriteLine($"launching v7.2: dotnet {args}");
        return Process.Start(psi);
    }

    private static async Task WaitForServerReady(string url, TimeSpan timeout)
    {
        using HttpClient http = new() { Timeout = TimeSpan.FromSeconds(5) };
        Stopwatch sw = Stopwatch.StartNew();
        Exception last = null;
        while (sw.Elapsed < timeout)
        {
            try
            {
                HttpResponseMessage resp = await http.GetAsync($"{url}/build/version");
                if (resp.IsSuccessStatusCode)
                    return;
            }
            catch (Exception e)
            {
                last = e;
            }
            await Task.Delay(1000);
        }
        throw new TimeoutException($"v7.2 server at {url} not ready within {timeout}. Last error: {last?.Message}");
    }

    private static void StopServer(Process p)
    {
        if (p == null || p.HasExited)
            return;
        try
        {
            p.Kill(entireProcessTree: true);
            p.WaitForExit(30_000);
        }
        catch
        {
            // ignore
        }
    }

    // ---- process runner -----------------------------------------------------

    private static (int Code, string Out, string Err) RunProcess(string exe, string args)
    {
        ProcessStartInfo psi = new(exe, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        using Process p = Process.Start(psi);
        string stdout = p.StandardOutput.ReadToEnd();
        string stderr = p.StandardError.ReadToEnd();
        p.WaitForExit();
        return (p.ExitCode, stdout, stderr);
    }
}
