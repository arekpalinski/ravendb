using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Tryouts.RavenDB_24520;

// Harness for RavenDB-24520: Shared Journals failure / corruption / recovery scenarios.
// Drives an EXTERNAL Raven.Server process so we can hard-kill it and corrupt files at rest.
// See test/RunBooks/RavenDB-24520/ for the runbooks + findings this produces.
public static class Harness
{
    public static readonly string BaseDir = Environment.GetEnvironmentVariable("RAVEN_24520_BASE") ?? @"D:\temp\24520";
    public static readonly string SourceDumpsDir = Environment.GetEnvironmentVariable("RAVEN_24520_DUMPS") ?? @"D:\workspace\stackoverflow-data-small";
    public static readonly string IndexesDumpFile = Environment.GetEnvironmentVariable("RAVEN_24520_INDEXES") ?? @"D:\workspace\stackoverflow-data\SO-indexes.ravendbdump";

    public static string GoldenDir => Path.Combine(BaseDir, "golden");
    public static string WorkDir => Path.Combine(BaseDir, "work");
    public static string StagingDir => Path.Combine(BaseDir, "staging-dumps");
    // Overridable: for a disk-full run the logs must be able to live OUTSIDE the volume being filled.
    // The VERDICT is derived by scanning these files, so if their own writes are failing with ENOSPC the
    // verdict is built on truncated evidence - and the harness used to die outright when that happened.
    public static string LogsDir => Environment.GetEnvironmentVariable("RAVEN_24520_LOGS") ?? Path.Combine(BaseDir, "logs");

    public const string DbName = "so";
    public const int Port = 8580;
    public static string Url => $"http://127.0.0.1:{Port}";

    // keep 1/SampleModulo of documents (by numeric id) so the whole DB stays small enough to copy per cell
    public static readonly int SampleModulo = int.Parse(Environment.GetEnvironmentVariable("RAVEN_24520_SAMPLE") ?? "50");

    // Small journals => many small files => faster per-cell restore + richer multi-file corruption topology.
    //
    // Overridable because the disk-full scenario needs it. At 16 MB the post-reset index rebuild fails on
    // index DATA-pager growth (Raven.voron, 16 -> 32 MB) long before enough index data accumulates to roll a
    // journal, so on Linux ENOSPC never reached the root's merged write and 27156 went unexercised across
    // five runs at leaveMB 60-500. A smaller journal rolls far more often, giving NextFile /
    // CreateJournalWriter a real chance to be the allocation that loses the race.
    public static readonly int MaxJournalFileSizeInMb = int.Parse(Environment.GetEnvironmentVariable("RAVEN_24520_JOURNAL_MB") ?? "16");

    public static async Task<int> RunAsync(string[] args)
    {
        var cmd = args.Length > 0 ? args[0].ToLowerInvariant() : "help";
        switch (cmd)
        {
            case "seed":
                await SeedAsync(dumps: args.Length > 1 ? int.Parse(args[1]) : 2);
                return 0;
            case "status":
                PrintStatus(args.Length > 1 ? args[1] : GoldenDir);
                return 0;
            case "map":
                JournalTools.PrintMap(args.Length > 1 ? args[1] : GoldenDir);
                return 0;
            case "restore-work":
                RestoreWorkDirFromGolden();
                return 0;
            case "cell":
                return await Scenarios.RunCellAsync(args);
            case "corrupt-live":
                return await Scenarios.RunCorruptLiveAsync(args);
            case "server":
                // manual mode: start a server on the work dir and leave it running until ENTER
                using (var s = ServerProcess.Start(args.Length > 1 ? args[1] : WorkDir))
                {
                    Console.WriteLine($"Server up at {Url}, data: {s.DataDir}. ENTER to kill.");
                    Console.ReadLine();
                }
                return 0;
            case "verify":
                return await VerifyAsync(args.Length > 1 ? args[1] : WorkDir) ? 0 : 1;
            case "diskfull":
                return await DiskFullAsync(args);
            default:
                Console.WriteLine("""
                    RavenDB-24520 harness commands:
                      seed [numPostsDumps]   stage dumps, seed golden dir (import + index + burst + hard kill)
                      status [dir]           print journals / hard-link topology of a data dir
                      map [dir]              print envs (JournalId, sync state) + per-inode tx maps
                      restore-work           reset work dir from golden (re-creating hard links)
                      cell <name> <op> <ownerFilter> <which> [fileSelector]
                                             restore-work -> corrupt one target -> verify -> record finding
                      corrupt-live <name> <op> <ownerFilter> <which> [fileSelector] [--probe passive|reset|both] [--observe <sec>]
                                             Scenario 1G (Linux): corrupt a journal WHILE the server holds it
                                             open, watch for live detection, then restart + verify
                      server [dir]           start server on dir, wait for ENTER, kill
                      verify [dir]           start server on dir, report DB load / index states / doc count
                      diskfull <dir> <leaveMB>  real disk-full drive against the index shared-journal path
                    """);
                return 1;
        }
    }

    // ---------------------------------------------------------------- seeding

    private static async Task SeedAsync(int dumps)
    {
        Console.WriteLine($"[seed] base={BaseDir} dumps={dumps} sample=1/{SampleModulo}");
        FreshDir(BaseDir);
        StageDumps(dumps);
        FreshDir(GoldenDir);

        using (var server = ServerProcess.Start(GoldenDir))
        {
            using var store = OpenStore();
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(DbName)));

            Console.WriteLine($"[seed] importing dumps from {StagingDir} (sampling 1/{SampleModulo}) ...");
            var sw = Stopwatch.StartNew();
            // These dumps are tagged BuildVersion=40000 (V4) but carry V3-era "Raven-Entity-Name" metadata,
            // so the server's legacy-key translation (V3 only) doesn't fire and every doc lands in @empty.
            // Promote it to @collection so the SO indexes (docs.Questions / docs.Users) match. Also SAMPLE
            // by numeric id so the whole DB stays small enough to copy per corruption cell.
            // import-dir can't carry a transform script, so import client-side per file.
            var importOptions = new DatabaseSmugglerImportOptions
            {
                TransformScript =
                    "var m = this['@metadata'];" +
                    "if (m['Raven-Entity-Name']) { m['@collection'] = m['Raven-Entity-Name']; }" +
                    "var id = m['@id']; var n = parseInt(id.substring(id.indexOf('/')+1));" +
                    $"if (isNaN(n) === false && (n % {SampleModulo}) !== 0) throw 'skip';"
            };
            foreach (var file in Directory.GetFiles(StagingDir, "*.dump").OrderBy(x => x))
            {
                var fileSw = Stopwatch.StartNew();
                var importOp = await store.Smuggler.ForDatabase(DbName).ImportAsync(importOptions, file);
                await importOp.WaitForCompletionAsync(TimeSpan.FromHours(2));
                Console.WriteLine($"[seed]   {Path.GetFileName(file)} in {fileSw.Elapsed}");
            }
            Console.WriteLine($"[seed] import done in {sw.Elapsed}");

            Console.WriteLine("[seed] importing index definitions ...");
            var op = await store.Smuggler.ForDatabase(DbName).ImportAsync(
                new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Indexes },
                IndexesDumpFile);
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            await WaitForIndexingAsync(store, TimeSpan.FromMinutes(60));

            var stats = await store.Maintenance.ForDatabase(DbName).SendAsync(new GetStatisticsOperation());
            var baseline = new Baseline
            {
                CountOfDocuments = stats.CountOfDocuments,
                Indexes = stats.Indexes.Select(i => i.Name).OrderBy(x => x).ToArray()
            };

            // Burst fresh Questions + Users (the collections the SO indexes actually cover) then wait until
            // they re-index. "non-stale" means the burst's index txs are in the journals; the periodic
            // journal->data sync hasn't fired yet, so a hard-kill now leaves UNSYNCED index txs that recovery
            // must replay - exactly the transactions corruption should be able to damage.
            Console.WriteLine("[seed] write burst (Questions + Users) ...");
            await WriteBurstAsync(store, 3_000);
            await WaitForIndexingAsync(store, TimeSpan.FromMinutes(10));
            baseline.CountOfDocumentsAfterBurst = (await store.Maintenance.ForDatabase(DbName).SendAsync(new GetStatisticsOperation())).CountOfDocuments;

            File.WriteAllText(Path.Combine(BaseDir, "baseline.json"), JsonSerializer.Serialize(baseline, new JsonSerializerOptions { WriteIndented = true }));

            Console.WriteLine("[seed] HARD KILL while journals are fresh");
            server.Kill();
        }

        WriteLinkManifest(GoldenDir);
        PrintStatus(GoldenDir);
        Console.WriteLine("[seed] done. Golden dir is now read-only reference - never open a server on it.");
    }

    private static void StageDumps(int postsDumps)
    {
        FreshDir(StagingDir);
        var posts = Directory.GetFiles(SourceDumpsDir, "posts-*.dump").OrderBy(x => x).Take(postsDumps).ToList();
        var users = Path.Combine(SourceDumpsDir, "users.dump");
        if (File.Exists(users))
            posts.Add(users);
        foreach (var src in posts)
        {
            var dst = Path.Combine(StagingDir, Path.GetFileName(src));
            if (TryHardLink(src, dst) == false)
                File.Copy(src, dst); // cross-volume fallback
        }
        Console.WriteLine($"[seed] staged {posts.Count} dump files");
    }

    public static async Task WriteBurstAsync(IDocumentStore store, int count, int startOffset = 0)
    {
        // write into the collections the SO indexes actually map over, so all 6 indexes get fresh work
        using (var bulk = store.BulkInsert(DbName, new BulkInsertOptions()))
        {
            for (int j = 0; j < count; j++)
            {
                var i = startOffset + j;
                await bulk.StoreAsync(new BurstQuestion
                {
                    Title = $"burst question {i} {Guid.NewGuid()}",
                    Body = string.Join(" ", Enumerable.Repeat($"payload-{i}", 40)),
                    Tags = new[] { "burst", $"tag-{i % 17}" },
                    CreationDate = DateTime.UtcNow.AddDays(-(i % 900)),
                    Score = i % 100,
                    OwnerUserId = i % 500,
                    LastEditorUserId = i % 500,
                    Answers = new[] { new BurstAnswer { OwnerUserId = i % 500 } }
                }, $"questions/burst-{i}", new Raven.Client.Json.MetadataAsDictionary { ["@collection"] = "Questions" });
            }
        }
        using (var bulk = store.BulkInsert(DbName, new BulkInsertOptions()))
        {
            for (int j = 0; j < count; j++)
            {
                var i = startOffset + j;
                await bulk.StoreAsync(new BurstUser
                {
                    DisplayName = $"burst user {i} {Guid.NewGuid()}",
                    Reputation = i % 1000,
                    CreationDate = DateTime.UtcNow.AddDays(-(i % 900))
                }, $"users/burst-{i}", new Raven.Client.Json.MetadataAsDictionary { ["@collection"] = "Users" });
            }
        }
    }

    private sealed class BurstQuestion
    {
        public string Title { get; set; }
        public string Body { get; set; }
        public string[] Tags { get; set; }
        public DateTime CreationDate { get; set; }
        public int Score { get; set; }
        public int OwnerUserId { get; set; }
        public int LastEditorUserId { get; set; }
        public BurstAnswer[] Answers { get; set; }
    }

    private sealed class BurstAnswer
    {
        public int OwnerUserId { get; set; }
    }

    private sealed class BurstUser
    {
        public string DisplayName { get; set; }
        public int Reputation { get; set; }
        public DateTime CreationDate { get; set; }
    }

    private sealed class Baseline
    {
        public long CountOfDocuments { get; set; }
        public long CountOfDocumentsAfterBurst { get; set; }
        public string[] Indexes { get; set; }
    }

    // ---------------------------------------------------------------- work dir restore (hard-link topology preserved)

    private sealed class LinkManifest
    {
        // groups of data-dir-relative paths that share one inode; first entry is the anchor
        public List<List<string>> Groups { get; set; } = new();
    }

    public static void WriteLinkManifest(string dataDir)
    {
        // group ALL journal files by inode - branch journals may share an inode with each other
        // even when the root's own link was already deleted (fully synced shared journal)
        var byInode = SharedJournalFiles(dataDir).Concat(BranchJournalFiles(dataDir))
            .GroupBy(GetFileId)
            .Where(g => g.Count() > 1)
            .Select(g => g.Select(f => Path.GetRelativePath(dataDir, f))
                // prefer the shared-journal file as anchor so relative order is stable
                .OrderBy(rel => rel.Contains("@SharedJournals") ? 0 : 1).ThenBy(rel => rel).ToList())
            .ToList();

        var manifest = new LinkManifest { Groups = byInode };
        File.WriteAllText(Path.Combine(dataDir, "link-manifest.json"), JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"[links] {manifest.Groups.Count} inode groups, {manifest.Groups.Sum(g => g.Count)} files");
    }

    public static void RestoreWorkDirFromGolden()
    {
        var sw = Stopwatch.StartNew();
        FreshDir(WorkDir);
        CopyTree(GoldenDir, WorkDir);

        var manifest = JsonSerializer.Deserialize<LinkManifest>(File.ReadAllText(Path.Combine(WorkDir, "link-manifest.json")));
        var relinked = 0;
        foreach (var group in manifest.Groups)
        {
            var anchor = Path.Combine(WorkDir, group[0]);
            foreach (var rel in group.Skip(1))
            {
                var abs = Path.Combine(WorkDir, rel);
                File.Delete(abs);
                if (TryHardLink(anchor, abs) == false)
                    throw new InvalidOperationException($"Failed to re-link {rel} -> {group[0]}");
                relinked++;
            }
        }
        File.Delete(Path.Combine(WorkDir, "link-manifest.json"));
        Console.WriteLine($"[restore-work] done in {sw.Elapsed}, {relinked} links re-created in {manifest.Groups.Count} groups");
    }

    // ---------------------------------------------------------------- verification

    public static async Task<bool> VerifyAsync(string dataDir)
    {
        var baseline = JsonSerializer.Deserialize<Baseline>(File.ReadAllText(Path.Combine(BaseDir, "baseline.json")));
        using var server = ServerProcess.Start(dataDir);
        using var store = OpenStore();

        var ok = true;
        try
        {
            var sw = Stopwatch.StartNew();
            DatabaseStatistics stats = null;
            Exception loadError = null;
            while (sw.Elapsed < TimeSpan.FromSeconds(75))
            {
                try
                {
                    stats = await store.Maintenance.ForDatabase(DbName).SendAsync(new GetStatisticsOperation());
                    loadError = null;
                    break;
                }
                catch (Exception e)
                {
                    loadError = e;
                    await Task.Delay(2000);
                }
            }

            if (stats == null)
            {
                Console.WriteLine($"[verify] DATABASE FAILED TO LOAD: {loadError?.GetType().Name}: {FirstLine(loadError?.Message)}");
                return false;
            }

            Console.WriteLine($"[verify] db loaded, docs={stats.CountOfDocuments} (baseline after burst: {baseline.CountOfDocumentsAfterBurst}), indexes={stats.CountOfIndexes}");
            if (stats.CountOfDocuments < baseline.CountOfDocuments)
            {
                Console.WriteLine("[verify] DOC COUNT BELOW BASELINE");
                ok = false;
            }

            var indexStats = await store.Maintenance.ForDatabase(DbName).SendAsync(new GetIndexesStatisticsOperation());
            foreach (var idx in indexStats)
            {
                var marker = idx.State == Raven.Client.Documents.Indexes.IndexState.Error || idx.IsInvalidIndex ? " <-- PROBLEM" : "";
                Console.WriteLine($"[verify]   {idx.Name,-60} state={idx.State} entries={idx.EntriesCount}{marker}");
                if (marker != "")
                    ok = false;
            }

            var missing = baseline.Indexes.Except(indexStats.Select(i => i.Name)).ToList();
            foreach (var m in missing)
            {
                Console.WriteLine($"[verify]   MISSING INDEX: {m}");
                ok = false;
            }

            var errors = await store.Maintenance.ForDatabase(DbName).SendAsync(new GetIndexErrorsOperation());
            foreach (var err in errors.Where(e => e.Errors.Length > 0))
            {
                ok = false;
                Console.WriteLine($"[verify]   index errors on {err.Name}:");
                foreach (var e in err.Errors.Take(3))
                    Console.WriteLine($"[verify]     {e.Error}");
            }
        }
        finally
        {
            server.Kill();
        }

        ScanServerLogs();
        Console.WriteLine($"[verify] => {(ok ? "OK" : "PROBLEMS FOUND (may be expected for the scenario)")}");
        return ok;
    }

    public static void ScanServerLogs()
    {
        if (Directory.Exists(LogsDir) == false)
            return;
        foreach (var file in Directory.GetFiles(LogsDir, "*.log"))
        {
            try
            {
                using var fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var reader = new StreamReader(fs);
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.Contains("ERROR") || line.Contains("FATAL") || (line.Contains("Recovery") && line.Contains("Error")))
                        Console.WriteLine($"[log] {Path.GetFileName(file)}: {line}");
                }
            }
            catch (IOException)
            {
                // log still held by a process; skip
            }
        }
    }

    public static void PrintStatus(string dataDir)
    {
        Console.WriteLine($"[status] {dataDir}");
        foreach (var f in SharedJournalFiles(dataDir))
            Console.WriteLine($"[status]   shared  {Path.GetRelativePath(dataDir, f),-70} {new FileInfo(f).Length,14:N0} links={GetHardLinkCount(f)}");
        foreach (var f in BranchJournalFiles(dataDir))
            Console.WriteLine($"[status]   branch  {Path.GetRelativePath(dataDir, f),-70} {new FileInfo(f).Length,14:N0} links={GetHardLinkCount(f)}");
    }

    public static IEnumerable<string> SharedJournalFiles(string dataDir)
    {
        var dir = Path.Combine(dataDir, "Databases", DbName, "Indexes", "@SharedJournals", "Journals");
        return Directory.Exists(dir) ? Directory.GetFiles(dir, "*.journal").OrderBy(x => x) : [];
    }

    public static IEnumerable<string> BranchJournalFiles(string dataDir)
    {
        var indexes = Path.Combine(dataDir, "Databases", DbName, "Indexes");
        if (Directory.Exists(indexes) == false)
            yield break;
        foreach (var indexDir in Directory.GetDirectories(indexes).Where(d => Path.GetFileName(d).StartsWith('@') == false).OrderBy(x => x))
        {
            var journals = Path.Combine(indexDir, "Journals");
            if (Directory.Exists(journals) == false)
                continue;
            foreach (var f in Directory.GetFiles(journals, "*.journal").OrderBy(x => x))
                yield return f;
        }
    }

    // ---------------------------------------------------------------- real disk-full

    // Real disk-full targeting the INDEX shared-journal write path: prime + let indexing settle while
    // space is available, then balloon the volume down to leaveMB, then force index rebuilds (RESET) so
    // the merger has to write a large volume to the shared journal with almost no free space. Documents env
    // is not written after ballooning, so the failure lands on the index/shared-journal side.
    // Usage: diskfull <dir> <balloonLeaveMB>
    private static async Task<int> DiskFullAsync(string[] args)
    {
        var dir = args.Length > 1 ? args[1] : WorkDir;
        var leaveMb = args.Length > 2 ? long.Parse(args[2]) : 30;
        var root = GetVolumeRoot(dir);
        var balloon = Path.Combine(root, "rdb24520-balloon.bin");
        // Print it: getting this wrong silently balloons the wrong device and produces an INCONCLUSIVE run
        // that reads like a pass. See GetVolumeRoot.
        Console.WriteLine($"[diskfull] data dir {dir} lives on volume '{root}'; balloon file {balloon}");

        // The VERDICT below is derived by counting matches across every *.log in LogsDir, so a previous run's
        // entries would be attributed to this one. That produced a false "ENOSPC reached and handled
        // gracefully" on 2026-08-07 for a run that never filled the disk at all. Start from an empty log dir.
        FreshDir(LogsDir);

        using var server = ServerProcess.Start(dir);
        using var store = OpenStore();

        Console.WriteLine("[diskfull] priming + settling indexing while space is available ...");
        // Must not be allowed to escape: if the volume is ALREADY full when priming starts - e.g. a previous
        // run aborted and left its balloon behind - BulkInsert throws, and an unhandled throw here kills the
        // harness before the balloon cleanup below, so the volume stays full and every subsequent run is
        // invalid too. Say so plainly and bail instead of cascading.
        try
        {
            await WriteBurstAsync(store, 8_000, startOffset: 200_000);
        }
        catch (Exception e)
        {
            Console.WriteLine($"[diskfull] PRIMING FAILED: {e.GetType().Name}: {FirstLine(e.Message)}");
            Console.WriteLine($"[diskfull] free space on '{root}' is {new DriveInfo(root).AvailableFreeSpace / 1024 / 1024}MB - " +
                              "the volume was already full before this run started, so the run is INVALID. " +
                              "Check for an orphaned rdb24520-balloon.bin from an earlier aborted run.");
            server.Kill();
            return 1;
        }
        try { await WaitForIndexingAsync(store, TimeSpan.FromMinutes(3)); } catch (Exception e) { Console.WriteLine($"[diskfull] pre-balloon indexing wait: {FirstLine(e.Message)}"); }

        var free = new DriveInfo(root).AvailableFreeSpace;
        var balloonSize = free - leaveMb * 1024 * 1024;
        Console.WriteLine($"[diskfull] free={free / 1024 / 1024}MB, ballooning {balloonSize / 1024 / 1024}MB to leave ~{leaveMb}MB");
        if (balloonSize > 0)
            AllocateBalloon(balloon, balloonSize);

        var freeAfter = new DriveInfo(root).AvailableFreeSpace;
        Console.WriteLine($"[diskfull] free after balloon: {freeAfter / 1024 / 1024}MB");

        // Fail loudly if the balloon did not actually take the space. A sparse balloon leaves the volume
        // empty, every write succeeds, and the run ends up reporting INCONCLUSIVE - which is easy to
        // misread as "the disk-full path is fine". Better to say the harness failed.
        if (balloonSize > 0 && freeAfter > leaveMb * 1024 * 1024 * 4)
        {
            Console.WriteLine($"[diskfull] BALLOON DID NOT CONSUME SPACE ({freeAfter / 1024 / 1024}MB still free after asking for {balloonSize / 1024 / 1024}MB). " +
                              "The run cannot reach ENOSPC - aborting instead of reporting a meaningless verdict.");
            DeleteBalloon(balloon);
            server.Kill();
            return 1;
        }

        string outcome = "no failure observed";
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            var indexes = (await store.Maintenance.ForDatabase(DbName).SendAsync(new GetIndexesStatisticsOperation())).Select(s => s.Name).ToList();
            Console.WriteLine($"[diskfull] resetting {indexes.Count} indexes to force shared-journal writes");
            foreach (var name in indexes)
            {
                using var req = new HttpRequestMessage(new HttpMethod("RESET"), $"{Url}/databases/{DbName}/indexes?name={Uri.EscapeDataString(name)}");
                try { await http.SendAsync(req); } catch (Exception e) { Console.WriteLine($"[diskfull] reset {name} threw: {FirstLine(e.Message)}"); }
            }

            for (int i = 0; i < 40; i++)
            {
                if (server.HasExited)
                {
                    outcome = "SERVER PROCESS DIED (ACCESS_VIOLATION / crash) during shared-journal write";
                    break;
                }
                await Task.Delay(1000);
                try
                {
                    var stats = await store.Maintenance.ForDatabase(DbName).SendAsync(new GetIndexesStatisticsOperation());
                    var errored = stats.Count(s => s.State == Raven.Client.Documents.Indexes.IndexState.Error);
                    if (i % 5 == 0)
                        Console.WriteLine($"[diskfull] t+{i}s errored={errored}/{stats.Length}");
                    if (errored > 0 && outcome == "no failure observed")
                        outcome = $"{errored} index(es) went to Error state (graceful disk-full handling)";
                }
                catch (Exception e)
                {
                    if (outcome == "no failure observed")
                        outcome = $"stats threw: {e.GetType().Name}: {FirstLine(e.Message)}";
                }
            }
        }
        catch (Exception e)
        {
            outcome = $"threw: {e.GetType().Name}: {FirstLine(e.Message)}";
        }

        var crashed = server.HasExited;
        Console.WriteLine($"[diskfull] index-state poller: {outcome}");
        Console.WriteLine($"[diskfull] server alive: {crashed == false}");

        DeleteBalloon(balloon);
        server.Kill();
        await Task.Delay(1500); // let the process release its log handle before scanning

        // The index-state poller alone is NOT the verdict: a disk-full usually surfaces in the logs
        // (DiskFullException / catastrophic failure -> DB unload) without any index sitting in Error
        // by the time we poll. Decide from the logs, and say so explicitly when the disk never
        // actually filled - otherwise an inconclusive run reads like a pass.
        var logHits = CountLogMatches("not enough space", "DiskFullException", "Errno: 112", "Errno: 28");
        // Match the log-LEVEL column, not the bare word: CountLogMatches is case-insensitive substring, and
        // the server's own startup banner ("Logging to '...' set to [Info, Fatal] level.") therefore counted
        // as a FATAL on every single run, inflating this number by 1 even when nothing fatal happened.
        var fatalHits = CountLogMatches("|FATAL|");
        Console.WriteLine($"[diskfull] VERDICT: {(crashed ? "SERVER CRASHED (F-3-class regression!)" : logHits > 0 ? $"ENOSPC reached and handled gracefully ({logHits} disk-full log entries, {fatalHits} FATAL, server alive)" : "INCONCLUSIVE - the disk never actually filled (no disk-full entries in the logs); re-run with a smaller leaveMB")}");

        ScanServerLogs();
        return 0;
    }

    // Leaving a balloon behind makes the volume permanently full, which silently invalidates every later
    // run against it - the failure mode that wrecked two runs of the 2026-08-10 sweep. Always reclaim it.
    private static void DeleteBalloon(string balloon)
    {
        try
        {
            if (File.Exists(balloon))
            {
                File.Delete(balloon);
                Console.WriteLine($"[diskfull] balloon released: {balloon}");
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"[diskfull] WARNING: could not delete the balloon {balloon}: {e.Message}. " +
                              "Delete it by hand before the next run or that run will be invalid.");
        }
    }

    public static int CountLogMatches(params string[] needles)
    {
        if (Directory.Exists(LogsDir) == false)
            return 0;
        var count = 0;
        foreach (var file in Directory.GetFiles(LogsDir, "*.log"))
        {
            try
            {
                using var fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var reader = new StreamReader(fs);
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    foreach (var n in needles)
                    {
                        if (line.Contains(n, StringComparison.OrdinalIgnoreCase))
                        {
                            count++;
                            break;
                        }
                    }
                }
            }
            catch (IOException)
            {
                // still held by a process; skip
            }
        }
        return count;
    }

    // ---------------------------------------------------------------- helpers

    public static DocumentStore OpenStore()
    {
        var store = new DocumentStore { Urls = [Url], Database = DbName };
        store.Conventions.RequestTimeout = TimeSpan.FromMinutes(10);
        store.Initialize();
        return store;
    }

    public static async Task WaitForIndexingAsync(IDocumentStore store, TimeSpan timeout)
    {
        // staleness may not register immediately after index creation / doc writes,
        // so require several consecutive clean polls before declaring victory
        var sw = Stopwatch.StartNew();
        var lastPrint = TimeSpan.Zero;
        var cleanPolls = 0;
        while (sw.Elapsed < timeout)
        {
            var stats = await store.Maintenance.ForDatabase(DbName).SendAsync(new GetStatisticsOperation());
            if (stats.CountOfIndexes > 0 && stats.StaleIndexes.Length == 0)
            {
                if (++cleanPolls >= 3 && sw.Elapsed > TimeSpan.FromSeconds(5))
                {
                    Console.WriteLine($"[indexing] all {stats.CountOfIndexes} indexes non-stale after {sw.Elapsed}");
                    return;
                }
            }
            else
            {
                cleanPolls = 0;
            }

            if (sw.Elapsed - lastPrint > TimeSpan.FromSeconds(15))
            {
                Console.WriteLine($"[indexing] {stats.StaleIndexes.Length}/{stats.CountOfIndexes} stale, {sw.Elapsed:mm\\:ss}");
                lastPrint = sw.Elapsed;
            }
            await Task.Delay(2000);
        }
        throw new TimeoutException($"Indexing did not settle in {timeout}");
    }

    private static string FirstLine(string s) => s?.Split('\n')[0].TrimEnd('\r');

    public static void FreshDir(string dir)
    {
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);
        Directory.CreateDirectory(dir);
    }

    private static void CopyTree(string src, string dst)
    {
        foreach (var dir in Directory.GetDirectories(src, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(dst, Path.GetRelativePath(src, dir)));
        foreach (var file in Directory.GetFiles(src, "*", SearchOption.AllDirectories))
            File.Copy(file, Path.Combine(dst, Path.GetRelativePath(src, file)));
    }

    // Creates a balloon file that REALLY consumes `size` bytes on its volume.
    //
    // FileStream.SetLength suffices on Windows, where NTFS allocates clusters as a file is extended, but it
    // is a no-op on Linux: it calls ftruncate(2), which only sets i_size and allocates no blocks. Measured
    // on ext4 here - a 1 GB SetLength moved available space by 0 MB, while posix_fallocate of the same size
    // consumed the full 1024 MB. Using SetLength on Linux means the volume never fills and the disk-full
    // scenario silently cannot reach ENOSPC, whatever leaveMB is set to.
    private static void AllocateBalloon(string path, long size)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            using var win = new FileStream(path, FileMode.Create, FileAccess.Write);
            win.SetLength(size);
            return;
        }

        using (var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write))
        {
            var rc = posix_fallocate((int)handle.DangerousGetHandle(), 0, size);
            if (rc == 0)
                return;
            Console.WriteLine($"[diskfull] posix_fallocate failed (rc={rc}); falling back to writing zeros");
        }

        // Fallback for filesystems without fallocate support: write real bytes.
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
        var buffer = new byte[8 * 1024 * 1024];
        long written = 0;
        while (written < size)
        {
            var chunk = (int)Math.Min(buffer.Length, size - written);
            fs.Write(buffer, 0, chunk);
            written += chunk;
        }
        fs.Flush(flushToDisk: true);
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int posix_fallocate(int fd, long offset, long len);

    // The volume that actually holds `path` - used to place the disk-full balloon and to measure free space.
    //
    // Path.GetPathRoot is correct on Windows ("C:\") but useless on Linux: it always returns "/", regardless
    // of which filesystem the data dir is on. That breaks the disk-full scenario twice over - the balloon
    // lands in "/", which a normal user cannot write (so the run dies before filling anything), and the free
    // space is measured on the root filesystem instead of the volume under test. Ask the OS for the real
    // mount point instead; `stat` is already a harness dependency (inode identity, link counts).
    public static string GetVolumeRoot(string path)
    {
        var full = Path.GetFullPath(path);
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return Path.GetPathRoot(full);

        var psi = new ProcessStartInfo("stat", $"-c %m \"{full}\"") { RedirectStandardOutput = true };
        using var p = Process.Start(psi);
        var output = p.StandardOutput.ReadToEnd().Trim();
        p.WaitForExit();
        return string.IsNullOrWhiteSpace(output) || Directory.Exists(output) == false
            ? Path.GetPathRoot(full)
            : output;
    }

    public static bool TryHardLink(string existing, string newLink)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return CreateHardLink(newLink, existing, IntPtr.Zero);
        return link(existing, newLink) == 0;
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    private static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

    [DllImport("libc", SetLastError = true)]
    private static extern int link(string oldpath, string newpath);

    public static uint GetHardLinkCount(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            using var handle = File.OpenHandle(path);
            if (GetFileInformationByHandle(handle, out var info) == false)
                return 0;
            return info.NumberOfLinks;
        }
        var psi = new ProcessStartInfo("stat", $"-c %h \"{path}\"") { RedirectStandardOutput = true };
        using var p = Process.Start(psi);
        var output = p.StandardOutput.ReadToEnd().Trim();
        p.WaitForExit();
        return uint.TryParse(output, out var n) ? n : 0;
    }

    // (VolumeSerial, FileIndex) uniquely identifies an inode on Windows; (dev, ino) via stat on Linux
    public static string GetFileId(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            using var handle = File.OpenHandle(path);
            if (GetFileInformationByHandle(handle, out var info) == false)
                throw new IOException($"GetFileInformationByHandle failed for {path}");
            return $"{info.VolumeSerialNumber}:{((ulong)info.FileIndexHigh << 32) | info.FileIndexLow}";
        }
        var psi = new ProcessStartInfo("stat", $"-c %d:%i \"{path}\"") { RedirectStandardOutput = true };
        using var p = Process.Start(psi);
        var output = p.StandardOutput.ReadToEnd().Trim();
        p.WaitForExit();
        return output;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4)] // native FILETIME is 4-byte aligned - no padding after FileAttributes
    private struct BY_HANDLE_FILE_INFORMATION
    {
        public uint FileAttributes;
        public long CreationTime;
        public long LastAccessTime;
        public long LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);
}

// ---------------------------------------------------------------- server process

public sealed class ServerProcess : IDisposable
{
    public string DataDir { get; }
    private Process _process;

    private ServerProcess(Process process, string dataDir)
    {
        _process = process;
        DataDir = dataDir;
    }

    public static ServerProcess Start(string dataDir, params string[] extraArgs)
    {
        var serverDll = FindServerDll();
        Directory.CreateDirectory(Harness.LogsDir);

        var args = new List<string>
        {
            serverDll,
            $"--ServerUrl={Harness.Url}",
            $"--DataDir={dataDir}",
            $"--Logs.Path={Harness.LogsDir}",
            "--Logs.MinLevel=Info", // NLog levels: Trace|Debug|Info|Warn|Error|Fatal|Off ("Information" is rejected)
            "--License.Eula.Accepted=true",
            "--Server.MaxTimeForTaskToWaitForDatabaseToLoadInSec=300",
            // small journals => many small files => faster per-cell restore + richer multi-file corruption topology
            $"--Storage.MaxJournalFileSizeInMb={Harness.MaxJournalFileSizeInMb}",
            "--non-interactive"
        };
        // extra server args from env (space-separated), e.g. dangerous recovery flags
        var extraFromEnv = Environment.GetEnvironmentVariable("RAVEN_24520_EXTRA_ARGS");
        if (string.IsNullOrWhiteSpace(extraFromEnv) == false)
            args.AddRange(extraFromEnv.Split(' ', StringSplitOptions.RemoveEmptyEntries));
        args.AddRange(extraArgs);

        var psi = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.GetDirectoryName(serverDll),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true
        };
        foreach (var a in args)
            psi.ArgumentList.Add(a);

        var stdout = Path.Combine(Harness.LogsDir, $"server-stdout-{DateTime.Now:HHmmss}.txt");
        var p = Process.Start(psi);
        var writer = new StreamWriter(stdout) { AutoFlush = true };
        // These handlers run on ThreadPool threads, so an exception here terminates the whole harness
        // process WITHOUT unwinding - no `using` disposal, so the server child is left orphaned and the
        // disk-full balloon is never deleted. That is exactly what happened during the disk-full sweep: the
        // volume being ballooned also held LogsDir, this writer hit ENOSPC, and the run died at SIGABRT
        // (exit 134), leaving a full volume that then invalidated the next two runs. Capturing stdout is
        // pure diagnostics; it must never be able to kill the run.
        p.OutputDataReceived += (_, e) => { TryWrite(writer, e.Data); };
        p.ErrorDataReceived += (_, e) => { TryWrite(writer, e.Data == null ? null : "[stderr] " + e.Data); };
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();

        WaitForAlive(p);
        Console.WriteLine($"[server] pid={p.Id} data={dataDir}");
        return new ServerProcess(p, dataDir);
    }

    private static void TryWrite(StreamWriter writer, string line)
    {
        if (line == null)
            return;
        try
        {
            writer.WriteLine(line);
        }
        catch (Exception)
        {
            // disk full / writer disposed - diagnostics only, never fatal (see the comment at the call site)
        }
    }

    private static void WaitForAlive(Process p)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromMinutes(2))
        {
            if (p.HasExited)
                throw new InvalidOperationException($"Server process exited with code {p.ExitCode} during startup, check {Harness.LogsDir}");
            try
            {
                var resp = http.GetAsync($"{Harness.Url}/setup/alive").Result;
                if (resp.IsSuccessStatusCode)
                    return;
            }
            catch
            {
                // not up yet
            }
            Thread.Sleep(500);
        }
        throw new TimeoutException("Server did not become alive within 2 minutes");
    }

    private static string FindServerDll()
    {
        // walk up from Tryouts output dir to the repo root, then to Raven.Server output
        var dir = AppDomain.CurrentDomain.BaseDirectory;
        while (dir != null && File.Exists(Path.Combine(dir, "RavenDB.slnx")) == false && File.Exists(Path.Combine(dir, "RavenDB.sln")) == false)
            dir = Path.GetDirectoryName(dir);
        if (dir == null)
            throw new InvalidOperationException("Could not locate repo root (RavenDB.slnx)");
        foreach (var config in new[] { "Release", "Debug" })
        {
            var candidate = Path.Combine(dir, "src", "Raven.Server", "bin", config, "net10.0", "Raven.Server.dll");
            if (File.Exists(candidate))
                return candidate;
        }
        throw new InvalidOperationException("Raven.Server.dll not found - build src/Raven.Server first");
    }

    public void Kill()
    {
        if (_process == null || _process.HasExited)
            return;
        _process.Kill(entireProcessTree: true);
        _process.WaitForExit();
        Console.WriteLine($"[server] killed pid={_process.Id}");
    }

    public bool HasExited => _process?.HasExited ?? true;

    public void Dispose()
    {
        Kill();
        _process?.Dispose();
        _process = null;
    }
}
