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
            return await IntegrityCheck(url, db, $"verify-{Guid.NewGuid():N}".Substring(0, 12));
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

        Console.WriteLine($"== carscenario mode={mode} queue={queue ?? "default"} iter={iterations} load={loadSeconds}s seed={seed} threads={threads} qa={qaDir}");
        if (!Directory.Exists(qaDir)) { Console.WriteLine("QA client dir not found: " + qaDir + " (set --qa-dir or QA_CLIENT_DIR)"); return 2; }
        FreshDir(dataDir);
        WriteQaAppConfig(qaDir, url);

        // SEED (once) - create DBs, deploy analyzer + indexes, seed docs
        Console.WriteLine("-- seed --");
        var seedSrv = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(90));
        if (seedSrv == null) { Console.WriteLine("  seed: server not ready"); return 1; }
        try
        {
            Console.WriteLine($"  selected WriteMode={await TryGetWriteMode(url)} (expected {mode})");
            var analyzer = Path.Combine(qaDir, "Databases", "RookDB-TMI-CORE-PROD", "CustomAnalyzer.cs");
            if (File.Exists(analyzer))
                Console.WriteLine($"  dca exit={await RunQaToEnd(qaDir, $"dca -p \"{analyzer}\" -n Rook.RavenAnalyzers.ASCIIAnalyzer -db RookDB-TMI-CORE-PROD", 120)}");
            foreach (var db in dbs)
                Console.WriteLine($"  di {db} exit={await RunQaToEnd(qaDir, $"di -db {db}", 180)}");
            foreach (var db in dbs)
                Console.WriteLine($"  dd {db} (n={seed}) exit={await RunQaToEnd(qaDir, $"dd -n {seed} -db {db}", 900)}");
            Console.WriteLine($"  seeded {primaryDb}: {await CountDocs(url, primaryDb)} docs");
        }
        finally { seedSrv.Kill(); }
        await Task.Delay(1000);

        // CRASH LOOP - real create/update/delete + query load, hard-kill mid-write, recover, integrity
        int passed = 0, failed = 0;
        for (int i = 1; i <= iterations; i++)
        {
            Console.WriteLine($"\n--- iteration {i}/{iterations} ---");
            var srv = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(120));
            if (srv == null) { failed++; continue; }
            Process ro = null, rq = null;
            try
            {
                ro = StartQa(qaDir, $"ro -th {threads} -mincs 1000 -maxcs 20000 -db {primaryDb}", echo: false);
                rq = StartQa(qaDir, $"rq -th {Math.Max(4, threads / 3)} -db {primaryDb}", echo: false);
                await Task.Delay(TimeSpan.FromSeconds(loadSeconds));
                Console.WriteLine($"  ~{await CountDocs(url, primaryDb)} docs in {primaryDb}; hard-killing server pid {srv.Pid} mid-write");
                srv.Kill();
            }
            finally
            {
                srv.Kill();
                try { ro?.Kill(entireProcessTree: true); } catch { }
                try { rq?.Kill(entireProcessTree: true); } catch { }
            }

            await Task.Delay(1000);
            var rec = await StartReady(mode, queue, url, dataDir, TimeSpan.FromSeconds(120));
            bool ok = false;
            if (rec == null) { Console.WriteLine("  RECOVERY FAILED"); failed++; continue; }
            try
            {
                ok = await IntegrityCheck(url, primaryDb, $"verify-{Guid.NewGuid():N}".Substring(0, 12));
            }
            finally { rec.Kill(); }
            if (ok) passed++; else failed++;
            Console.WriteLine($"iteration {i}: {(ok ? "PASS" : "FAIL")} (passed={passed} failed={failed})");
        }

        Console.WriteLine($"\n== carscenario summary mode={mode} queue={queue ?? "default"}: {passed} passed, {failed} failed");
        return failed == 0 ? 0 : 1;
    }

    private static void WriteQaAppConfig(string qaDir, string url)
    {
        File.WriteAllText(Path.Combine(qaDir, "appConfig.json"), $"{{\"Urls\":[\"{url}\"],\"CertFilePath\":null}}");
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
        bool ok = await IntegrityCheck(url, db, $"verify-{Guid.NewGuid():N}".Substring(0, 12));
        return ok ? 0 : 1;
    }

    // ---- integrity -------------------------------------------------------

    private static async Task<bool> IntegrityCheck(string url, string sourceDb, string verifyDb)
    {
        var file = Path.Combine(Path.GetTempPath(), $"rdb24528-{verifyDb}.ravendump");
        using var store = new DocumentStore { Urls = new[] { url }, Database = sourceDb }.Initialize();
        try
        {
            long sourceCount = await CountDocs(url, sourceDb);
            var export = await store.Smuggler.ForDatabase(sourceDb).ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await export.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(verifyDb)));
            var import = await store.Smuggler.ForDatabase(verifyDb).ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await import.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            long verifyCount = await CountDocs(url, verifyDb);
            bool match = sourceCount == verifyCount;
            Console.WriteLine($"  integrity: export+import OK, docs source={sourceCount} verify={verifyCount} -> {(match ? "MATCH" : "MISMATCH")}");
            return match;
        }
        catch (Exception e)
        {
            Console.WriteLine("  integrity FAILED: " + e.Message);
            return false;
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

    private static ServerHandle StartServer(string mode, string queue, string url, string dataDir)
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
            $"--Testing.ParentProcessId={Process.GetCurrentProcess().Id}",
            $"--Storage.WriteMode={mode}"
        };
        if (!string.IsNullOrEmpty(queue))
            args.Add($"--Storage.IoRingQueueSize={queue}");

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
    private static async Task<ServerHandle> StartReady(string mode, string queue, string url, string dataDir, TimeSpan timeout, int attempts = 4)
    {
        ServerHandle s = null;
        for (int a = 1; a <= attempts; a++)
        {
            s = StartServer(mode, queue, url, dataDir);
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
        for (int attempt = 0; attempt < 12; attempt++)
        {
            try
            {
                using var store = new DocumentStore { Urls = new[] { url }, Database = db }.Initialize();
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

    private static string DefaultDataDir(string name) => Path.Combine(Path.GetTempPath(), "ravendb-24528", name);

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
            _settingsFile = Path.Combine(Path.GetTempPath(), "rdb24528-empty-settings.json");
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
