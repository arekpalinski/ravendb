using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using System.Linq;

namespace Tryouts;

// RavenDB-24528 Voron v8 WriteMode / IoRingQueueSize test orchestrator.
// Launches the real Raven.Server as a child process so each run gets a fresh
// process-global PalConfiguration and can be hard-killed. See test/RunBooks/RavenDB-24528.
public static class Program
{
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(10) };
    private static readonly string[] ModeNames = { "Auto", "VectoredFileIo", "FileIo", "IoRing", "Mmap" };

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 1;
        }

        try
        {
            switch (args[0].ToLowerInvariant())
            {
                case "node-info":
                    return await NodeInfoCommand(args.Length > 1 ? args[1] : "http://127.0.0.1:8080");
                case "scenario":
                    return await ScenarioCommand(ParseOptions(args));
                case "carscenario":
                    return await CarScenarioCommand(ParseOptions(args));
                case "numbers-seed":
                    return await NumbersSeedCommand(ParseOptions(args));
                case "numbers-scenario":
                    return await NumbersScenarioCommand(ParseOptions(args));
                case "negative":
                    return await NegativeCommand(ParseOptions(args));
                case "integrity":
                    return await IntegrityCommand(ParseOptions(args));
                default:
                    PrintUsage();
                    return 1;
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("FATAL: " + e);
            return 2;
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("RavenDB-24528 orchestrator. Commands:");
        Console.WriteLine("  node-info <url>");
        Console.WriteLine("  scenario  --mode <M> [--queue <N>] [--iterations <K>] [--load-seconds <S>] [--url <U>] [--data <DIR>]");
        Console.WriteLine("  carscenario --mode <M> [--queue <N>] [--iterations <K>] [--load-seconds <S>] [--seed-docs <N>] [--threads <N>] [--qa-dir <DIR>] [--url <U>] [--data <DIR>]");
        Console.WriteLine("  numbers-seed [--db NumbersAndUnits] [--count <N/coll>] [--indexes <dump>] [--mode <M>] [--workers <N>] [--keep-running] [--url <U>] [--data <DIR>]");
        Console.WriteLine("  numbers-scenario --mode <M> [--queue <N>] [--iterations <K>] [--load-seconds <S>] [--seed-count <N>] [--final-integrity] [--url <U>] [--data <DIR>]");
        Console.WriteLine("  negative  --mode <M> [--queue <N>] [--url <U>] [--data <DIR>]");
        Console.WriteLine("  integrity --url <U> --db <NAME>");
        Console.WriteLine("Server path auto-discovered; override with RAVEN_SERVER_PATH.");
    }

    // ---- commands --------------------------------------------------------

    private static async Task<int> NodeInfoCommand(string url)
    {
        var mode = await TryGetWriteMode(url);
        Console.WriteLine(mode == null ? $"node-info unreachable at {url}" : $"WriteMode = {mode}");
        return mode == null ? 1 : 0;
    }

    private static async Task<int> ScenarioCommand(Dictionary<string, string> o)
    {
        var mode = o.GetValueOrDefault("mode") ?? throw new ArgumentException("--mode required");
        var queue = o.GetValueOrDefault("queue");
        var iterations = int.Parse(o.GetValueOrDefault("iterations") ?? "25");
        var loadSeconds = int.Parse(o.GetValueOrDefault("load-seconds") ?? "30");
        var url = o.GetValueOrDefault("url") ?? "http://127.0.0.1:8080";
        var dataDir = o.GetValueOrDefault("data") ?? DefaultDataDir($"scenario-{mode}-{queue ?? "def"}");
        const string db = "workload";

        Console.WriteLine($"== scenario mode={mode} queue={queue ?? "default"} iterations={iterations} load={loadSeconds}s data={dataDir}");
        FreshDir(dataDir);

        int passed = 0, failed = 0;
        for (int i = 1; i <= iterations; i++)
        {
            Console.WriteLine($"\n--- iteration {i}/{iterations} ---");
            bool ok = await RunOneCrashCycle(mode, queue, url, dataDir, db, loadSeconds, firstIteration: i == 1);
            if (ok) passed++; else failed++;
            Console.WriteLine($"iteration {i}: {(ok ? "PASS" : "FAIL")}  (passed={passed} failed={failed})");
            if (!ok)
                Console.WriteLine("  -> FAILURE: stop and investigate (data dir preserved at " + dataDir + ")");
        }

        Console.WriteLine($"\n== summary mode={mode} queue={queue ?? "default"}: {passed} passed, {failed} failed");
        return failed == 0 ? 0 : 1;
    }

    private static async Task<bool> RunOneCrashCycle(string mode, string queue, string url, string dataDir, string db, int loadSeconds, bool firstIteration)
    {
        // 1. start, load, crash mid-write
        var s1 = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(90));
        if (s1 == null)
            return false;
        try
        {
            if (firstIteration)
            {
                var actual = await TryGetWriteMode(url);
                Console.WriteLine($"  selected WriteMode = {actual} (expected {mode})");
                if (!string.Equals(actual, mode, StringComparison.OrdinalIgnoreCase))
                    Console.WriteLine("  WARNING: selected mode != requested mode (record this).");
                EnsureDatabase(url, db);
            }

            using var cts = new CancellationTokenSource();
            var load = RunLoad(url, db, cts.Token);
            await Task.Delay(TimeSpan.FromSeconds(loadSeconds));
            long before = await CountDocs(url, db);
            Console.WriteLine($"  ~{before} docs written; hard-killing pid {s1.Pid} mid-write");
            s1.Kill();
            cts.Cancel();
            await SwallowAsync(load);
        }
        finally
        {
            s1.Kill();
        }

        // 2. restart (recovery) + integrity
        await Task.Delay(1000);
        var s2 = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(120));
        if (s2 == null)
        {
            Console.WriteLine("  RECOVERY FAILED: server did not restart");
            return false;
        }
        try
        {
            return (await IntegrityCheck(url, db, $"verify-{Guid.NewGuid():N}".Substring(0, 12))).ok;
        }
        finally
        {
            s2.Kill();
        }
    }

    // ---- car-dealership scenario (real QA workload client) --------------

    private static async Task<int> CarScenarioCommand(Dictionary<string, string> o)
    {
        var mode = o.GetValueOrDefault("mode") ?? throw new ArgumentException("--mode required");
        var queue = o.GetValueOrDefault("queue");
        var encrypt = o.ContainsKey("encrypt");
        var varyKill = o.ContainsKey("vary-kill");
        int[] killDelays = { 3, 7, 12, 20, 30 };
        var iterations = int.Parse(o.GetValueOrDefault("iterations") ?? "10");
        var loadSeconds = int.Parse(o.GetValueOrDefault("load-seconds") ?? "60");
        var seed = int.Parse(o.GetValueOrDefault("seed-docs") ?? "2000");
        var threads = int.Parse(o.GetValueOrDefault("threads") ?? "25");
        var url = o.GetValueOrDefault("url") ?? "http://127.0.0.1:8080";
        var dataDir = o.GetValueOrDefault("data") ?? DefaultDataDir($"car-{mode}-{queue ?? "def"}");
        var qaDir = o.GetValueOrDefault("qa-dir") ?? Environment.GetEnvironmentVariable("QA_CLIENT_DIR")
            ?? @"D:\workspace\ravendb-qa-workload-client\QAWorkloadClient\bin\Release\net8.0";
        string[] dbs = { "RookDB-TMI-PROD", "RookDB-TMI-CORE-PROD" };
        var primaryDb = dbs[0];

        Console.WriteLine($"== carscenario mode={mode} queue={queue ?? "default"} encrypt={encrypt} iter={iterations} load={loadSeconds}s seed={seed} threads={threads} data={dataDir} qa={qaDir}");
        if (!Directory.Exists(qaDir)) { Console.WriteLine("QA client dir not found: " + qaDir + " (set --qa-dir or QA_CLIENT_DIR)"); return 2; }
        FreshDir(dataDir);
        WriteQaAppConfig(qaDir, url);
        var key = (string.IsNullOrEmpty(queue) ? mode : $"{mode}-{queue}") + (encrypt ? "-enc" : "") + (varyKill ? "-vk" : "");
        Progress.Update(key, r => { r.Mode = mode; r.Queue = queue; r.IterTotal = iterations; r.Phase = "seeding"; });
        Console.WriteLine($"  dashboard: {Progress.HtmlFile}");

        // SEED (once) - create DBs, deploy analyzer + indexes, seed docs
        Console.WriteLine("-- seed --");
        var seedSrv = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(90), logDir: Path.Combine(dataDir, "logs", "seed"));
        if (seedSrv == null) { Console.WriteLine("  seed: server not ready"); Progress.Update(key, r => r.Phase = "seed-FAILED"); return 1; }
        try
        {
            var selected = await TryGetWriteMode(url);
            Console.WriteLine($"  selected WriteMode={selected} (expected {mode})");
            Progress.Update(key, r => r.Selected = selected);
            if (encrypt)
            {
                Progress.Update(key, r => r.Phase = "seed: encrypting");
                await Http.PostAsync($"{url}/admin/cluster/bootstrap", null); // promote node so PutSecretKey works on a non-passive node
                await Task.Delay(1000);
                using var es = new DocumentStore { Urls = new[] { url } }.Initialize();
                foreach (var db in dbs)
                {
                    var sk = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
                    var resp = await Http.PostAsync($"{url}/admin/secrets?name={db}&overwrite=true", new StringContent(sk));
                    es.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(db) { Encrypted = true }));
                    Console.WriteLine($"  encrypted {db}: putSecret={(int)resp.StatusCode} Encrypted=true");
                }
            }
            var analyzer = Path.Combine(qaDir, "Databases", "RookDB-TMI-CORE-PROD", "CustomAnalyzer.cs");
            if (File.Exists(analyzer))
            {
                Progress.Update(key, r => r.Phase = "seed: analyzer");
                Console.WriteLine($"  dca exit={await RunQaToEnd(qaDir, $"dca -p \"{analyzer}\" -n Rook.RavenAnalyzers.ASCIIAnalyzer -db RookDB-TMI-CORE-PROD", 120)}");
            }
            foreach (var db in dbs)
            {
                Progress.Update(key, r => r.Phase = $"seed: indexes {db}");
                Console.WriteLine($"  di {db} exit={await RunQaToEnd(qaDir, $"di -db {db}", 180)}");
            }
            foreach (var db in dbs)
            {
                Progress.Update(key, r => r.Phase = $"seed: docs {db}");
                Console.WriteLine($"  dd {db} (n={seed}) exit={await RunQaToEnd(qaDir, $"dd -n {seed} -db {db}", 900)}");
            }
            var seededCount = await CountDocs(url, primaryDb);
            Console.WriteLine($"  seeded {primaryDb}: {seededCount} docs");
            Progress.Update(key, r => { r.Phase = "seeded"; r.Note = $"seed {seededCount} docs"; });
        }
        finally { seedSrv.Kill(); }
        await Task.Delay(1000);

        // CRASH LOOP - real create/update/delete + query load, hard-kill mid-write, recover, integrity
        int passed = 0, failed = 0;
        for (int i = 1; i <= iterations; i++)
        {
            Console.WriteLine($"\n--- iteration {i}/{iterations} ---");
            var loadLogDir = Path.Combine(dataDir, "logs", $"load-{i}");
            var recoverLogDir = Path.Combine(dataDir, "logs", $"recover-{i}");
            long lastDelta = 0;
            int itIdxErr = 0, itLogErr = 0;
            string note = null;

            var srv = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(120), logDir: loadLogDir, logLevel: "Warn");
            if (srv == null)
            {
                failed++;
                Progress.Update(key, r => { r.IterDone = i; r.Failed = failed; r.Phase = $"iter {i}/{iterations}"; r.Note = "load server not ready"; });
                continue;
            }
            Process ro = null, rq = null;
            try
            {
                long baseline = await CountDocs(url, primaryDb); // warms the DB (loads indexes) so ro/rq don't hit a cold DB and exit
                ro = StartQa(qaDir, $"ro -th {threads} -mincs 1000 -maxcs 20000 -db {primaryDb}", echo: false);
                rq = StartQa(qaDir, $"rq -th {Math.Max(4, threads / 3)} -db {primaryDb}", echo: false);
                int thisLoad = varyKill ? killDelays[(i - 1) % killDelays.Length] : loadSeconds;
                await Task.Delay(TimeSpan.FromSeconds(thisLoad));
                long beforeKill = await CountDocs(url, primaryDb);
                lastDelta = beforeKill - baseline;
                Console.WriteLine($"  {primaryDb} {baseline} -> {beforeKill} docs (+{lastDelta} in {thisLoad}s); hard-killing server pid {srv.Pid} mid-write");
                srv.Kill();
            }
            finally
            {
                srv.Kill();
                try { ro?.Kill(entireProcessTree: true); } catch { }
                try { rq?.Kill(entireProcessTree: true); } catch { }
            }

            await Task.Delay(1000);
            var rec = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(120), logDir: recoverLogDir);
            bool ok = false;
            if (rec == null)
            {
                Console.WriteLine("  RECOVERY FAILED");
                failed++;
                Progress.Update(key, r => { r.IterDone = i; r.Failed = failed; r.LastDelta = lastDelta; r.Phase = $"iter {i}/{iterations}"; r.Note = "RECOVERY FAILED (no restart)"; });
                continue;
            }
            try
            {
                var idx = await CheckIndexes(url, primaryDb);
                var integ = await IntegrityCheck(url, primaryDb, $"verify-{Guid.NewGuid():N}".Substring(0, 12));
                var logs = ScanLogErrors(recoverLogDir);
                itIdxErr = Math.Max(0, idx.errorIndexes) + Math.Max(0, idx.indexErrors);
                itLogErr = logs.errors;
                bool idxOk = idx.errorIndexes <= 0 && idx.indexErrors <= 0;
                ok = integ.ok && idxOk && logs.errors == 0;
                if (idx.errorIndexes > 0 || idx.indexErrors > 0)
                {
                    Console.WriteLine($"  INDEX PROBLEM: {idx.errorIndexes} index(es) in Error state, {idx.indexErrors} index error(s) [{idx.sample}]");
                    note = $"index: {idx.errorIndexes} errored, {idx.indexErrors} errs [{idx.sample}]";
                }
                if (logs.errors > 0 || logs.warns > 0)
                    Console.WriteLine($"  recovery log: {logs.errors} ERROR/FATAL, {logs.warns} WARN");
                foreach (var s in logs.samples)
                    Console.WriteLine($"    [reclog] {s}");
                if (logs.errors > 0)
                    note = (note == null ? "" : note + "; ") + $"{logs.errors} recovery ERROR/FATAL" + (logs.samples.Count > 0 ? ": " + logs.samples[0] : "");
                if (!integ.ok)
                    note = (note == null ? "" : note + "; ") + integ.detail;
            }
            finally { rec.Kill(); }
            if (ok) passed++; else failed++;
            Console.WriteLine($"iteration {i}: {(ok ? "PASS" : "FAIL")} (passed={passed} failed={failed})");
            Progress.Update(key, r =>
            {
                r.IterDone = i; r.Passed = passed; r.Failed = failed; r.LastDelta = lastDelta;
                r.IndexErrors += itIdxErr; r.LogErrors += itLogErr; r.Phase = $"iter {i}/{iterations}";
                if (note != null) r.Note = note;
            });
        }

        Console.WriteLine($"\n== carscenario summary mode={mode} queue={queue ?? "default"}: {passed} passed, {failed} failed");
        Progress.Update(key, r => r.Phase = "done");
        return failed == 0 ? 0 : 1;
    }

    private static void WriteQaAppConfig(string qaDir, string url)
    {
        File.WriteAllText(Path.Combine(qaDir, "appConfig.json"), $"{{\"Urls\":[\"{url}\"],\"CertFilePath\":null}}");
    }

    // ---- "Numbers and units" dataset: bulk-load Numbers + Users collections + import indexes from dump ----

    private sealed class NumberDoc { public long PartId { get; set; } public double PriceValue { get; set; } public int Count { get; set; } }
    private sealed class UserDoc { public string Date { get; set; } public int Count { get; set; } }

    private static async Task<int> NumbersSeedCommand(Dictionary<string, string> o)
    {
        var url = o.GetValueOrDefault("url") ?? "http://127.0.0.1:8080";
        var db = o.GetValueOrDefault("db") ?? "NumbersAndUnits";
        var count = int.Parse(o.GetValueOrDefault("count") ?? "5000000");
        var mode = o.GetValueOrDefault("mode") ?? "Auto";
        var workers = int.Parse(o.GetValueOrDefault("workers") ?? "8");
        var dataDir = o.GetValueOrDefault("data") ?? DefaultDataDir("numbers-data");
        var indexes = o.GetValueOrDefault("indexes") ?? @"D:\workspace\ravendb-dumps\Numbers_and_units-Indexes.ravendbdump";
        var keepRunning = o.ContainsKey("keep-running");

        Console.WriteLine($"== numbers-seed db={db} count={count:N0}/collection mode={mode} workers={workers} data={dataDir}");
        FreshDir(dataDir);
        var srv = await StartReady(mode, null, url, dataDir, TimeSpan.FromSeconds(90));
        if (srv == null) { Console.WriteLine("  server not ready"); return 1; }
        try
        {
            Console.WriteLine($"  selected WriteMode={await TryGetWriteMode(url)}");
            using var store = new DocumentStore { Urls = new[] { url }, Database = db, Conventions = { RequestTimeout = TimeSpan.FromMinutes(5) } };
            store.Conventions.FindCollectionName = type =>
                type == typeof(NumberDoc) ? "Numbers" :
                type == typeof(UserDoc) ? "Users" :
                Raven.Client.Documents.Conventions.DocumentConventions.DefaultGetCollectionName(type);
            store.Initialize();
            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(db)));

            var sw = Stopwatch.StartNew();
            await BulkLoad(store, "Numbers", count, workers, (rnd, i) => new NumberDoc { PartId = 200_000_000L + rnd.Next(0, 50_000_000), PriceValue = Math.Round(rnd.NextDouble() * 1000.0, 2), Count = 0 });
            Console.WriteLine($"  Numbers: {count:N0} inserted in {sw.Elapsed}");
            sw.Restart();
            var baseDate = new DateTime(2023, 1, 1);
            await BulkLoad(store, "Users", count, workers, (rnd, i) => new UserDoc { Date = baseDate.AddDays(rnd.Next(0, 730)).ToString("yyyy-MM-dd"), Count = rnd.Next(0, 5) });
            Console.WriteLine($"  Users: {count:N0} inserted in {sw.Elapsed}");

            if (File.Exists(indexes))
            {
                Console.WriteLine($"  importing indexes from {indexes}");
                var opts = new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Indexes };
                var op = await store.Smuggler.ForDatabase(db).ImportAsync(opts, indexes);
                await op.WaitForCompletionAsync(TimeSpan.FromMinutes(10));
                Console.WriteLine("  indexes imported (build in background)");
            }
            else Console.WriteLine($"  WARNING: indexes dump not found at {indexes} - skipped");

            var stats = store.Maintenance.Send(new GetCollectionStatisticsOperation());
            Console.WriteLine("  collections: " + string.Join(", ", stats.Collections.Select(kv => $"{kv.Key}={kv.Value:N0}")));
            Console.WriteLine($"  total docs: {stats.CountOfDocuments:N0}");
        }
        finally { if (!keepRunning) srv.Kill(); }
        Console.WriteLine(keepRunning ? $"  server left RUNNING at {url} (data {dataDir})" : $"  dataset created at {dataDir} (server stopped; data persists for reuse)");
        return 0;
    }

    private static async Task BulkLoad<T>(IDocumentStore store, string label, int total, int workers, Func<Random, int, T> gen)
    {
        long done = 0;
        using var cts = new CancellationTokenSource();
        var reporter = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try { await Task.Delay(5000, cts.Token); } catch { break; }
                Console.WriteLine($"  [{label}] ~{Interlocked.Read(ref done):N0}/{total:N0}");
            }
        });
        int per = total / workers;
        var tasks = new List<Task>();
        for (int w = 0; w < workers; w++)
        {
            int wi = w;
            tasks.Add(Task.Run(async () =>
            {
                var rnd = new Random(wi * 9973 + 12345);
                int start = wi * per;
                int end = (wi == workers - 1) ? total : start + per;
                await using var bulk = store.BulkInsert();
                for (int i = start; i < end; i++)
                {
                    await bulk.StoreAsync(gen(rnd, i));
                    if ((i & 4095) == 0) Interlocked.Add(ref done, 4096);
                }
            }));
        }
        await Task.WhenAll(tasks);
        cts.Cancel();
        try { await reporter; } catch { }
    }

    private static string NumbersCollectionName(Type t) =>
        t == typeof(NumberDoc) ? "Numbers" : t == typeof(UserDoc) ? "Users" : Raven.Client.Documents.Conventions.DocumentConventions.DefaultGetCollectionName(t);

    // continuous Numbers/Users insert load (sessions, survives mid-write kill) until cancelled
    private static Task NumbersLoad(string url, string db, CancellationToken ct, int workers)
    {
        var tasks = new List<Task>();
        for (int w = 0; w < workers; w++)
        {
            int wi = w;
            tasks.Add(Task.Run(async () =>
            {
                using var store = new DocumentStore { Urls = new[] { url }, Database = db, Conventions = { FindCollectionName = NumbersCollectionName } }.Initialize();
                var rnd = new Random(wi * 7919 + 7);
                var baseDate = new DateTime(2023, 1, 1);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var s = store.OpenAsyncSession();
                        for (int i = 0; i < 256 && !ct.IsCancellationRequested; i++)
                        {
                            if ((i & 1) == 0)
                                await s.StoreAsync(new NumberDoc { PartId = 200_000_000L + rnd.Next(0, 50_000_000), PriceValue = Math.Round(rnd.NextDouble() * 1000.0, 2), Count = 0 });
                            else
                                await s.StoreAsync(new UserDoc { Date = baseDate.AddDays(rnd.Next(0, 730)).ToString("yyyy-MM-dd"), Count = rnd.Next(0, 5) });
                        }
                        await s.SaveChangesAsync(ct);
                    }
                    catch (OperationCanceledException) { break; }
                    catch { /* server killed mid-write or transient */ }
                }
            }, ct));
        }
        return Task.WhenAll(tasks);
    }

    private static async Task<int> NumbersScenarioCommand(Dictionary<string, string> o)
    {
        var mode = o.GetValueOrDefault("mode") ?? throw new ArgumentException("--mode required");
        var queue = o.GetValueOrDefault("queue");
        var iterations = int.Parse(o.GetValueOrDefault("iterations") ?? "3");
        var loadSeconds = int.Parse(o.GetValueOrDefault("load-seconds") ?? "20");
        var seedCount = int.Parse(o.GetValueOrDefault("seed-count") ?? "5000000");
        var workers = int.Parse(o.GetValueOrDefault("workers") ?? "8");
        var finalIntegrity = o.ContainsKey("final-integrity");
        var url = o.GetValueOrDefault("url") ?? "http://127.0.0.1:8080";
        var db = o.GetValueOrDefault("db") ?? "NumbersAndUnits";
        var dataDir = o.GetValueOrDefault("data") ?? DefaultDataDir($"numbers-{mode}-{queue ?? "def"}");
        var indexes = o.GetValueOrDefault("indexes") ?? @"D:\workspace\ravendb-dumps\Numbers_and_units-Indexes.ravendbdump";
        var key = (string.IsNullOrEmpty(queue) ? mode : $"{mode}-{queue}") + "-num";

        Console.WriteLine($"== numbers-scenario mode={mode} queue={queue ?? "default"} iter={iterations} load={loadSeconds}s seed={seedCount:N0} finalIntegrity={finalIntegrity} data={dataDir}");
        FreshDir(dataDir);
        Progress.Update(key, r => { r.Mode = mode; r.Queue = queue; r.IterTotal = iterations; r.Phase = "seeding"; });

        var seedSrv = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(90), logDir: Path.Combine(dataDir, "logs", "seed"));
        if (seedSrv == null) { Console.WriteLine("  seed: server not ready"); Progress.Update(key, r => r.Phase = "seed-FAILED"); return 1; }
        try
        {
            var selected = await TryGetWriteMode(url);
            Console.WriteLine($"  selected WriteMode={selected} (expected {mode})");
            Progress.Update(key, r => r.Selected = selected);
            using (var store = new DocumentStore { Urls = new[] { url }, Database = db, Conventions = { FindCollectionName = NumbersCollectionName, RequestTimeout = TimeSpan.FromMinutes(5) } }.Initialize())
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(db)));
                Progress.Update(key, r => r.Phase = "seed: Numbers");
                await BulkLoad(store, "Numbers", seedCount, workers, (rnd, i) => new NumberDoc { PartId = 200_000_000L + rnd.Next(0, 50_000_000), PriceValue = Math.Round(rnd.NextDouble() * 1000.0, 2), Count = 0 });
                Progress.Update(key, r => r.Phase = "seed: Users");
                var bd = new DateTime(2023, 1, 1);
                await BulkLoad(store, "Users", seedCount, workers, (rnd, i) => new UserDoc { Date = bd.AddDays(rnd.Next(0, 730)).ToString("yyyy-MM-dd"), Count = rnd.Next(0, 5) });
                if (File.Exists(indexes))
                {
                    Progress.Update(key, r => r.Phase = "seed: indexes");
                    var op = await store.Smuggler.ForDatabase(db).ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Indexes }, indexes);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(10));
                }
            }
            var seeded = await CountDocs(url, db);
            Console.WriteLine($"  seeded {db}: {seeded:N0} docs");
            Progress.Update(key, r => { r.Phase = "seeded"; r.Note = $"seed {seeded:N0}"; });
        }
        finally { seedSrv.Kill(); }
        await Task.Delay(1000);

        int passed = 0, failed = 0;
        for (int i = 1; i <= iterations; i++)
        {
            Console.WriteLine($"\n--- iteration {i}/{iterations} ---");
            var recoverLogDir = Path.Combine(dataDir, "logs", $"recover-{i}");
            long baseline = 0, beforeKill = 0;
            string note = null;
            var srv = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(180), logDir: Path.Combine(dataDir, "logs", $"load-{i}"), logLevel: "Warn");
            if (srv == null) { failed++; Progress.Update(key, r => { r.IterDone = i; r.Failed = failed; r.Phase = $"iter {i}/{iterations}"; r.Note = "load server not ready"; }); continue; }
            using var cts = new CancellationTokenSource();
            Task load = null;
            try
            {
                baseline = await CountDocs(url, db);
                load = NumbersLoad(url, db, cts.Token, workers);
                await Task.Delay(TimeSpan.FromSeconds(loadSeconds));
                beforeKill = await CountDocs(url, db);
                Console.WriteLine($"  {db} {baseline:N0} -> {beforeKill:N0} (+{beforeKill - baseline:N0} in {loadSeconds}s); hard-killing pid {srv.Pid} mid-write");
                srv.Kill();
                cts.Cancel();
            }
            finally { srv.Kill(); cts.Cancel(); await SwallowAsync(load); }

            await Task.Delay(1000);
            var rec = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(180), logDir: recoverLogDir);
            bool ok = false;
            if (rec == null) { Console.WriteLine("  RECOVERY FAILED"); failed++; Progress.Update(key, r => { r.IterDone = i; r.Failed = failed; r.Phase = $"iter {i}/{iterations}"; r.Note = "RECOVERY FAILED"; }); continue; }
            try
            {
                long recovered = await CountDocs(url, db);
                var idx = await CheckIndexes(url, db);
                var logs = ScanLogErrors(recoverLogDir);
                bool countOk = recovered >= baseline; // committed pre-crash docs must survive (dataset must not shrink)
                bool idxOk = idx.errorIndexes <= 0 && idx.indexErrors <= 0;
                ok = countOk && idxOk && logs.errors == 0;
                Console.WriteLine($"  recovered={recovered:N0} (baseline {baseline:N0}, beforeKill {beforeKill:N0}); idxErr={idx.errorIndexes} logErr={logs.errors} -> {(ok ? "PASS" : "FAIL")}");
                if (!countOk) note = $"COUNT LOSS recovered {recovered:N0} < baseline {baseline:N0}";
                if (idx.errorIndexes > 0 || idx.indexErrors > 0) { note = (note == null ? "" : note + "; ") + $"index: {idx.errorIndexes} errored [{idx.sample}]"; }
                if (logs.errors > 0) { note = (note == null ? "" : note + "; ") + $"{logs.errors} recovery ERROR/FATAL"; foreach (var s in logs.samples) Console.WriteLine($"    [reclog] {s}"); }
                long delta = beforeKill - baseline;
                Progress.Update(key, r => { r.IterDone = i; r.Passed = (ok ? passed + 1 : passed); r.Failed = (ok ? failed : failed + 1); r.LastDelta = delta; r.IndexErrors += Math.Max(0, idx.errorIndexes); r.LogErrors += logs.errors; r.Phase = $"iter {i}/{iterations}"; if (note != null) r.Note = note; });
            }
            finally { rec.Kill(); }
            if (ok) passed++; else failed++;
        }

        if (finalIntegrity)
        {
            Console.WriteLine("\n-- final integrity (export/import on full dataset) --");
            Progress.Update(key, r => r.Phase = "final integrity");
            var fs = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(180));
            if (fs != null)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var integ = await IntegrityCheck(url, db, $"verify-{Guid.NewGuid():N}".Substring(0, 12));
                    Console.WriteLine($"  final integrity: {(integ.ok ? "MATCH" : "MISMATCH " + integ.detail)} ({sw.Elapsed})");
                    if (!integ.ok) { failed++; Progress.Update(key, r => { r.Failed = failed; r.Note = "final " + integ.detail; }); }
                }
                finally { fs.Kill(); }
            }
        }

        Console.WriteLine($"\n== numbers-scenario summary mode={mode}: {passed} passed, {failed} failed");
        Progress.Update(key, r => r.Phase = "done");
        return failed == 0 ? 0 : 1;
    }

    private static (string fileName, string argPrefix) QaLauncher(string qaDir)
    {
        var exe = Path.Combine(qaDir, OperatingSystem.IsWindows() ? "QAWorkloadClient.exe" : "QAWorkloadClient");
        if (File.Exists(exe)) return (exe, "");
        var dll = Path.Combine(qaDir, "QAWorkloadClient.dll");
        if (File.Exists(dll)) return ("dotnet", $"\"{dll}\" ");
        throw new FileNotFoundException($"QAWorkloadClient not found in {qaDir}");
    }

    private static Process StartQa(string qaDir, string argsString, bool echo = true)
    {
        var (fileName, prefix) = QaLauncher(qaDir);
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = prefix + argsString,
            WorkingDirectory = qaDir,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        var p = new Process { StartInfo = psi };
        p.OutputDataReceived += (_, e) => { if (e.Data != null && echo) Console.WriteLine("  [qa] " + e.Data); };
        p.ErrorDataReceived += (_, e) => { if (e.Data != null && echo) Console.WriteLine("  [qa-err] " + e.Data); };
        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        return p;
    }

    private static async Task<int> RunQaToEnd(string qaDir, string argsString, int timeoutSec)
    {
        var p = StartQa(qaDir, argsString);
        var exited = await Task.Run(() => p.WaitForExit(timeoutSec * 1000));
        if (!exited) { try { p.Kill(entireProcessTree: true); } catch { } return -1; }
        return p.ExitCode;
    }

    private static async Task<int> NegativeCommand(Dictionary<string, string> o)
    {
        var mode = o.GetValueOrDefault("mode") ?? throw new ArgumentException("--mode required");
        var queue = o.GetValueOrDefault("queue");
        var url = o.GetValueOrDefault("url") ?? "http://127.0.0.1:8080";
        var dataDir = o.GetValueOrDefault("data") ?? DefaultDataDir($"negative-{mode}-{queue ?? "def"}");
        FreshDir(dataDir);

        Console.WriteLine($"== negative mode={mode} queue={queue ?? "default"} (expect fail-to-start)");
        var s = StartServer(mode, queue, url, dataDir);
        try
        {
            // race: process exit (expected) vs node-info up (unexpected start)
            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (DateTime.UtcNow < deadline)
            {
                if (s.HasExited)
                {
                    Console.WriteLine($"RESULT: server FAILED TO START as expected (exit code {s.ExitCode}).");
                    Console.WriteLine("---- captured server output (tail) ----\n" + s.Tail());
                    return 0;
                }
                if (await TryGetWriteMode(url) is { } m)
                {
                    Console.WriteLine($"RESULT: server STARTED (did NOT fail). WriteMode={m}. FINDING - record this.");
                    return 1;
                }
                await Task.Delay(500);
            }
            Console.WriteLine("RESULT: inconclusive - server neither exited nor served node-info within 60s.");
            Console.WriteLine("---- captured server output (tail) ----\n" + s.Tail());
            return 1;
        }
        finally
        {
            s.Kill();
        }
    }

    private static async Task<int> IntegrityCommand(Dictionary<string, string> o)
    {
        var url = o.GetValueOrDefault("url") ?? "http://127.0.0.1:8080";
        var db = o.GetValueOrDefault("db") ?? throw new ArgumentException("--db required");
        bool ok = (await IntegrityCheck(url, db, $"verify-{Guid.NewGuid():N}".Substring(0, 12))).ok;
        return ok ? 0 : 1;
    }

    // ---- integrity -------------------------------------------------------

    private static async Task<(bool ok, string detail)> IntegrityCheck(string url, string sourceDb, string verifyDb)
    {
        var file = Path.Combine(TempBase(), $"rdb24528-{verifyDb}.ravendump");
        using var store = new DocumentStore { Urls = new[] { url }, Database = sourceDb, Conventions = { RequestTimeout = TimeSpan.FromMinutes(3) } }.Initialize();
        try
        {
            long sourceCount = await CountDocs(url, sourceDb);
            var export = await store.Smuggler.ForDatabase(sourceDb).ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await export.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(verifyDb)));
            var import = await store.Smuggler.ForDatabase(verifyDb).ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await import.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            long verifyCount = await CountDocs(url, verifyDb);
            bool match = sourceCount >= 0 && sourceCount == verifyCount;
            Console.WriteLine($"  integrity: export+import OK, docs source={sourceCount} verify={verifyCount} -> {(match ? "MATCH" : "MISMATCH")}");
            return (match, match ? null : $"count source={sourceCount} verify={verifyCount}");
        }
        catch (Exception e)
        {
            Console.WriteLine("  integrity FAILED: " + e.Message);
            return (false, "integrity error: " + e.GetType().Name);
        }
        finally
        {
            try { store.Maintenance.Server.Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters { DatabaseNames = new[] { verifyDb }, HardDelete = true })); } catch { }
            try { if (File.Exists(file)) File.Delete(file); } catch { }
        }
    }

    // ---- load generator --------------------------------------------------

    private sealed class Doc
    {
        public string Name { get; set; }
        public int N { get; set; }
        public string Payload { get; set; }
        public DateTime Ts { get; set; }
    }

    private static Task RunLoad(string url, string db, CancellationToken ct)
    {
        const int workers = 16;
        var tasks = new List<Task>();
        for (int w = 0; w < workers; w++)
        {
            int seed = w;
            tasks.Add(Task.Run(async () =>
            {
                using var store = new DocumentStore { Urls = new[] { url }, Database = db }.Initialize();
                var payloadSmall = new string('x', 200);
                var payloadBig = new string('y', 40_000); // > one Voron page, exercises multi-page writes
                int n = seed * 1_000_000;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var session = store.OpenAsyncSession(db);
                        for (int i = 0; i < 50 && !ct.IsCancellationRequested; i++)
                        {
                            await session.StoreAsync(new Doc
                            {
                                Name = $"w{seed}-{n}",
                                N = n,
                                Payload = (n % 20 == 0) ? payloadBig : payloadSmall,
                                Ts = DateTime.UtcNow
                            });
                            n++;
                        }
                        await session.SaveChangesAsync(ct);
                    }
                    catch (OperationCanceledException) { break; }
                    catch { /* server killed mid-write or transient - expected */ }
                }
            }, ct));
        }
        return Task.WhenAll(tasks);
    }

    // ---- server process --------------------------------------------------

    private sealed class ServerHandle
    {
        public Process Process;
        private readonly ConcurrentQueue<string> _lines = new();

        public int Pid => Process.Id;
        public bool HasExited { get { try { return Process.HasExited; } catch { return true; } } }
        public int ExitCode { get { try { return Process.ExitCode; } catch { return -1; } } }

        public void Record(string line)
        {
            if (line == null) return;
            _lines.Enqueue(line);
            while (_lines.Count > 80 && _lines.TryDequeue(out _)) { }
        }

        public string Tail() => string.Join(Environment.NewLine, _lines);

        public void Kill()
        {
            try { if (!Process.HasExited) Process.Kill(entireProcessTree: true); } catch { }
            try { Process.WaitForExit(10000); } catch { } // ensure handles (incl. mmap journals) are released before restart
        }
    }

    private static ServerHandle StartServer(string mode, string queue, string url, string dataDir, string logDir = null, string logLevel = "Info")
    {
        var serverPath = ServerExecutable();
        var settings = EmptySettingsFile();
        var args = new List<string>
        {
            $"-c \"{settings}\"",
            $"--ServerUrl={url}",
            $"--DataDir=\"{dataDir}\"",
            "--RunInMemory=false",
            "--Setup.Mode=None",
            "--License.Eula.Accepted=true",
            "--Security.UnsecuredAccessAllowed=PublicNetwork",
            "--Features.Availability=Experimental",
            "--Server.MaxTimeForTaskToWaitForDatabaseToLoadInSec=120",
            $"--Testing.ParentProcessId={Process.GetCurrentProcess().Id}",
            $"--Storage.WriteMode={mode}"
        };
        if (!string.IsNullOrEmpty(queue))
            args.Add($"--Storage.IoRingQueueSize={queue}");
        if (!string.IsNullOrEmpty(logDir))
        {
            Directory.CreateDirectory(logDir);
            args.Add($"--Logs.Path=\"{logDir}\"");
            args.Add($"--Logs.MinLevel={logLevel}");
        }

        var psi = new ProcessStartInfo
        {
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        if (serverPath.dll != null)
        {
            psi.FileName = "dotnet";
            psi.Arguments = $"\"{serverPath.dll}\" " + string.Join(" ", args);
        }
        else
        {
            psi.FileName = serverPath.exe;
            psi.Arguments = string.Join(" ", args);
        }

        var handle = new ServerHandle();
        var p = new Process { StartInfo = psi };
        p.OutputDataReceived += (_, e) => { if (e.Data != null) { handle.Record(e.Data); Console.WriteLine("  [srv] " + e.Data); } };
        p.ErrorDataReceived += (_, e) => { if (e.Data != null) { handle.Record(e.Data); Console.WriteLine("  [srv-err] " + e.Data); } };
        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        handle.Process = p;
        return handle;
    }

    private static async Task<bool> WaitReady(string url, ServerHandle s, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (s.HasExited)
                return false;
            if (await TryGetWriteMode(url) != null)
                return true;
            await Task.Delay(500);
        }
        return false;
    }

    // Start the server and wait until ready, retrying on the transient "journal file in use" race
    // that can happen when a freshly-killed server with many environments hasn't released handles yet.
    private static async Task<ServerHandle> StartReady(string mode, string queue, string url, string dataDir, TimeSpan timeout, int attempts = 4, string logDir = null, string logLevel = "Info")
    {
        ServerHandle s = null;
        for (int a = 1; a <= attempts; a++)
        {
            s = StartServer(mode, queue, url, dataDir, logDir, logLevel);
            if (await WaitReady(url, s, timeout))
                return s;
            s.Kill();
            if (a < attempts)
            {
                Console.WriteLine($"  server start attempt {a}/{attempts} failed (likely handle release); retrying");
                await Task.Delay(2000 * a);
            }
        }
        Console.WriteLine("  server failed to start after retries; tail:\n" + s?.Tail());
        return null;
    }

    // ---- helpers ---------------------------------------------------------

    private static async Task<string> TryGetWriteMode(string url)
    {
        try
        {
            var json = await Http.GetStringAsync($"{url.TrimEnd('/')}/cluster/node-info");
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("WriteMode", out var wm))
            {
                if (wm.ValueKind == JsonValueKind.Number)
                {
                    int idx = wm.GetInt32();
                    return idx >= 0 && idx < ModeNames.Length ? ModeNames[idx] : idx.ToString();
                }
                return wm.GetString();
            }
            return "?(no WriteMode field)";
        }
        catch
        {
            return null;
        }
    }

    private static async Task<long> CountDocs(string url, string db)
    {
        // retry: a freshly-recovered DB (esp. with many indexes) may not be online for a few seconds
        for (int attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                using var store = new DocumentStore { Urls = new[] { url }, Database = db, Conventions = { RequestTimeout = TimeSpan.FromMinutes(3) } }.Initialize();
                var stats = await Task.Run(() => store.Maintenance.ForDatabase(db).Send(new GetCollectionStatisticsOperation()));
                return stats.CountOfDocuments;
            }
            catch
            {
                await Task.Delay(2000);
            }
        }
        return -1;
    }

    // Post-recovery index health: any index in Error state, plus total index errors.
    private static async Task<(int errorIndexes, int indexErrors, string sample)> CheckIndexes(string url, string db)
    {
        try
        {
            using var store = new DocumentStore { Urls = new[] { url }, Database = db }.Initialize();
            var stats = await Task.Run(() => store.Maintenance.ForDatabase(db).Send(new GetStatisticsOperation()));
            var errored = stats.Indexes.Where(i => i.State == IndexState.Error).Select(i => i.Name).ToArray();
            int totalErrors = 0;
            try
            {
                var errs = await Task.Run(() => store.Maintenance.ForDatabase(db).Send(new GetIndexErrorsOperation()));
                totalErrors = errs.Sum(e => e.Errors.Length);
            }
            catch { }
            return (errored.Length, totalErrors, string.Join(", ", errored.Take(5)));
        }
        catch (Exception e)
        {
            return (-1, -1, e.Message);
        }
    }

    // Scan a server session's log dir for ERROR/FATAL (and count WARN). Layout: longdate|LEVEL|... (Sparrow Constants.DefaultLayout)
    private static (int errors, int warns, List<string> samples) ScanLogErrors(string logDir)
    {
        var samples = new List<string>();
        int errors = 0, warns = 0;
        try
        {
            if (Directory.Exists(logDir) == false)
                return (0, 0, samples);
            foreach (var f in Directory.GetFiles(logDir, "*.log"))
            {
                string text;
                try
                {
                    using var fs = new FileStream(f, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                    using var sr = new StreamReader(fs);
                    text = sr.ReadToEnd();
                }
                catch { continue; }
                foreach (var line in text.Split('\n'))
                {
                    int p1 = line.IndexOf('|');
                    if (p1 < 0) continue;
                    int p2 = line.IndexOf('|', p1 + 1);
                    if (p2 < 0) continue;
                    var level = line.Substring(p1 + 1, p2 - p1 - 1);
                    if (level == "ERROR" || level == "FATAL")
                    {
                        errors++;
                        if (samples.Count < 8) samples.Add(line.Length > 300 ? line.Substring(0, 300) : line.TrimEnd());
                    }
                    else if (level == "WARN")
                    {
                        warns++;
                    }
                }
            }
        }
        catch { }
        return (errors, warns, samples);
    }

    // ---- live HTML progress dashboard (progress.html under TempBase, auto-refresh) -------

    public sealed class ModeRow
    {
        public string Key { get; set; }
        public string Mode { get; set; }
        public string Queue { get; set; }
        public string Selected { get; set; }
        public string Phase { get; set; }
        public int IterTotal { get; set; }
        public int IterDone { get; set; }
        public int Passed { get; set; }
        public int Failed { get; set; }
        public long LastDelta { get; set; }
        public int IndexErrors { get; set; }
        public int LogErrors { get; set; }
        public string Note { get; set; }
    }

    private static class Progress
    {
        private static string StateFile => Path.Combine(TempBase(), "progress.json");
        public static string HtmlFile => Path.Combine(TempBase(), "progress.html");

        // Named mutex: safe across the parallel carscenario processes that share progress.json.
        public static void Update(string key, Action<ModeRow> mutate)
        {
            using var mutex = new Mutex(false, "RDB24528Progress");
            bool held = false;
            try { held = mutex.WaitOne(TimeSpan.FromSeconds(15)); } catch (AbandonedMutexException) { held = true; } catch { }
            try
            {
                List<ModeRow> rows;
                try { rows = File.Exists(StateFile) ? JsonSerializer.Deserialize<List<ModeRow>>(File.ReadAllText(StateFile)) : new List<ModeRow>(); }
                catch { rows = new List<ModeRow>(); }
                rows ??= new List<ModeRow>();
                var row = rows.FirstOrDefault(r => r.Key == key);
                if (row == null) { row = new ModeRow { Key = key }; rows.Add(row); }
                mutate(row);
                try { File.WriteAllText(StateFile, JsonSerializer.Serialize(rows)); } catch { }
                try { File.WriteAllText(HtmlFile, BuildHtml(rows)); } catch { }
            }
            finally { if (held) { try { mutex.ReleaseMutex(); } catch { } } }
        }

        private static string BuildHtml(List<ModeRow> rows)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("<!doctype html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"3\">");
            sb.Append("<title>RavenDB-24528 matrix</title><style>");
            sb.Append("body{background:#0d1117;color:#c9d1d9;font:14px Segoe UI,system-ui,sans-serif;margin:24px}");
            sb.Append("h1{font-size:18px;margin:0 0 4px}.sub{color:#8b949e;font-size:12px;margin-bottom:16px}");
            sb.Append("table{border-collapse:collapse;width:100%}th,td{padding:9px 12px;border-bottom:1px solid #21262d;text-align:left;white-space:nowrap}");
            sb.Append("th{color:#8b949e;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.04em}");
            sb.Append(".ok{color:#3fb950}.bad{color:#f85149;font-weight:700}.run{color:#d29922}");
            sb.Append(".bar{display:inline-block;height:8px;width:110px;background:#21262d;border-radius:4px;overflow:hidden;vertical-align:middle;margin-right:8px}");
            sb.Append(".bar>i{display:block;height:100%;background:#3fb950}.note{color:#8b949e;white-space:normal;max-width:420px;font-size:12px}");
            sb.Append("</style></head><body>");
            sb.Append("<h1>RavenDB-24528 - Voron v8 WriteMode car-dealership matrix</h1>");
            sb.Append($"<div class=\"sub\">updated {DateTime.Now:yyyy-MM-dd HH:mm:ss} - auto-refresh 3s</div>");
            sb.Append("<table><tr><th>Mode</th><th>Selected</th><th>Phase</th><th>Progress</th><th>Pass</th><th>Fail</th><th>Last &#916;docs</th><th>Idx err</th><th>Log err</th><th>Note</th></tr>");
            foreach (var r in rows)
            {
                bool bad = r.Failed > 0 || r.IndexErrors > 0 || r.LogErrors > 0;
                string cls = bad ? "bad" : (r.Phase == "done" ? "ok" : "run");
                int pct = r.IterTotal > 0 ? (int)(100.0 * r.IterDone / r.IterTotal) : 0;
                string label = r.Mode + (string.IsNullOrEmpty(r.Queue) ? "" : " q" + r.Queue) + (r.Key != null && r.Key.Contains("-enc") ? " (enc)" : "") + (r.Key != null && r.Key.Contains("-vk") ? " (vary-kill)" : "") + (r.Key != null && r.Key.Contains("-num") ? " (numbers)" : "");
                sb.Append($"<tr><td>{H(label)}</td><td>{H(r.Selected)}</td><td class=\"{cls}\">{H(r.Phase)}</td>");
                sb.Append($"<td><span class=\"bar\"><i style=\"width:{pct}%\"></i></span>{r.IterDone}/{r.IterTotal}</td>");
                sb.Append($"<td class=\"ok\">{r.Passed}</td><td class=\"{(r.Failed > 0 ? "bad" : "")}\">{r.Failed}</td>");
                sb.Append($"<td>{r.LastDelta}</td><td class=\"{(r.IndexErrors > 0 ? "bad" : "")}\">{r.IndexErrors}</td><td class=\"{(r.LogErrors > 0 ? "bad" : "")}\">{r.LogErrors}</td>");
                sb.Append($"<td class=\"note\">{H(r.Note)}</td></tr>");
            }
            sb.Append("</table></body></html>");
            return sb.ToString();
        }

        private static string H(string s) => string.IsNullOrEmpty(s) ? "" : System.Net.WebUtility.HtmlEncode(s);
    }

    private static void EnsureDatabase(string url, string db)
    {
        using var store = new DocumentStore { Urls = new[] { url }, Database = db }.Initialize();
        try { store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(db))); }
        catch (Exception e) when (e.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase)) { }
    }

    private static async Task SwallowAsync(Task t)
    {
        try { await t; } catch { }
    }

    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        var o = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 1; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--")) continue;
            var key = args[i].Substring(2);
            var val = (i + 1 < args.Length && !args[i + 1].StartsWith("--")) ? args[++i] : "true";
            o[key] = val;
        }
        return o;
    }

    // Base for all test artifacts. D: is faster on the Windows test box; fall back to system temp elsewhere (incl. Linux).
    private static string TempBase()
    {
        var baseDir = OperatingSystem.IsWindows() && Directory.Exists(@"D:\") ? @"D:\temp\ravendb-24528" : Path.Combine(Path.GetTempPath(), "ravendb-24528");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private static string DefaultDataDir(string name) => Path.Combine(TempBase(), name);

    private static void FreshDir(string dir)
    {
        try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); } catch { }
        Directory.CreateDirectory(dir);
    }

    private static string _settingsFile;
    private static string EmptySettingsFile()
    {
        if (_settingsFile == null)
        {
            _settingsFile = Path.Combine(TempBase(), "rdb24528-empty-settings.json");
            File.WriteAllText(_settingsFile, "{}");
        }
        return _settingsFile;
    }

    private static (string exe, string dll) ServerExecutable()
    {
        var env = Environment.GetEnvironmentVariable("RAVEN_SERVER_PATH");
        if (!string.IsNullOrEmpty(env) && File.Exists(env))
            return env.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) ? (null, env) : (env, null);

        var repo = FindRepoRoot();
        var dir = Path.Combine(repo, "src", "Raven.Server", "bin", "Release", "net10.0");
        var exe = Path.Combine(dir, OperatingSystem.IsWindows() ? "Raven.Server.exe" : "Raven.Server");
        if (File.Exists(exe))
            return (exe, null);
        var dll = Path.Combine(dir, "Raven.Server.dll");
        if (File.Exists(dll))
            return (null, dll);
        throw new FileNotFoundException($"Raven.Server not found under {dir}. Build the solution or set RAVEN_SERVER_PATH.");
    }

    private static string FindRepoRoot()
    {
        var d = new DirectoryInfo(AppContext.BaseDirectory);
        while (d != null && !File.Exists(Path.Combine(d.FullName, "RavenDB.sln")))
            d = d.Parent;
        return d?.FullName ?? Directory.GetCurrentDirectory();
    }
}
