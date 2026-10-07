using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Voron;
using Voron.Impl.Journal;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// b-shared-kill: a root and 3 branch environments share journals while journals roll, get recycled and are
    /// handed over by the branches to the root's pool. A child process is killed at seeded random moments. On odd
    /// iterations branch 1 starts standalone and switches to shared journals after a few transactions.
    /// After recovery: every acknowledged transaction of every environment is present, recovered hard-linked journals
    /// are never a write target, branches keep no reusable journal files, and the root's pool matches the files on disk.
    /// </summary>
    public static class BSharedKill
    {
        private const int Branches = 3;
        private const int ItemsPerTx = 20;
        private const int TxsBetweenFlushSync = 10;
        private const int TxsBetweenShadowWrites = 25;
        private const int StandaloneTxsBeforeSharing = 30;
        private const int ChildLifetimeSeconds = 30;

        public static int Run(StressContext ctx)
        {
            if (ctx.IsChild)
                return RunChild(ctx);

            var rng = new Random(ctx.Seed);
            for (var iteration = 0; ctx.TimeLeft; iteration++)
            {
                ctx.Iteration = iteration;
                ctx.Iterations++;
                var dir = ctx.IterationDir(iteration);
                Directory.CreateDirectory(dir);

                ctx.RunChildAndKill(iteration, rng);
                Verify(ctx, dir);

                if (ctx.Failures.Count > 0)
                    break; // keep the evidence of the first failing iteration

                Directory.Delete(dir, recursive: true);
            }

            return ctx.Finish();
        }

        private static int RunChild(StressContext ctx)
        {
            var dir = ctx.IterationDir(ctx.Iteration);
            var deadline = Stopwatch.StartNew();

            using var root = new StorageEnvironment(CreateOptions(RootDir(dir), rootJournal: null, prewarm: true));
            root.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;
            using var scope = root.Journal.SharedJournalsScope();
            using var merger = new Merger(root);

            Console.WriteLine($"child up, pid {Environment.ProcessId}, dir {dir}");

            var writers = new List<Thread> { StartWriter(dir, "root", () => Write(root, RootDir(dir), "root", 0, deadline)) };
            for (var b = 1; b <= Branches; b++)
            {
                var branch = b;
                var startStandalone = branch == 1 && ctx.Iteration % 2 == 1;
                writers.Add(StartWriter(dir, BranchName(branch), () => RunBranch(dir, branch, root, startStandalone, deadline)));
            }

            foreach (var writer in writers)
                writer.Join();

            return 0;
        }

        private static void RunBranch(string dir, int branch, StorageEnvironment root, bool startStandalone, Stopwatch deadline)
        {
            var branchDir = BranchDir(dir, branch);
            long committed = 0;
            if (startStandalone)
            {
                using var standalone = new StorageEnvironment(CreateOptions(branchDir, rootJournal: null, prewarm: true));
                committed = Write(standalone, branchDir, BranchName(branch), committed, deadline, stopAfter: StandaloneTxsBeforeSharing);
            }

            using var shared = new StorageEnvironment(CreateOptions(branchDir, root.Journal, prewarm: true));
            Write(shared, branchDir, BranchName(branch), committed, deadline);
        }

        private static long Write(StorageEnvironment env, string envDir, string name, long committed, Stopwatch deadline, long stopAfter = long.MaxValue)
        {
            using var shadow = new FileStream(ShadowPath(envDir), FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);

            while (deadline.Elapsed.TotalSeconds < ChildLifetimeSeconds && committed < stopAfter)
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("data");
                    for (var i = 0; i < ItemsPerTx; i++)
                    {
                        var n = committed * ItemsPerTx + i + 1;
                        tree.Add(KeyOf(n), ValueOf(name, n));
                    }

                    tx.CreateTree("meta").Add("count", BitConverter.GetBytes(committed + 1));
                    tx.Commit(); // sync commit - durable when this returns
                }

                committed++;

                if (committed % TxsBetweenFlushSync == 0)
                {
                    env.FlushLogToDataFile();
                    using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                        sync.SyncDataFile();
                }

                if (committed % TxsBetweenShadowWrites == 0)
                {
                    shadow.Position = 0;
                    shadow.Write(BitConverter.GetBytes(committed));
                    shadow.Flush(flushToDisk: true);
                }
            }

            return committed;
        }

        private static Thread StartWriter(string dir, string name, Action write)
        {
            var thread = new Thread(() =>
            {
                try
                {
                    write();
                }
                catch (Exception e)
                {
                    // the parent checks for this file - a writer that dies before the kill is a finding, not noise
                    File.AppendAllText(Path.Combine(dir, "writer-failed.txt"), $"{name}: {e}{Environment.NewLine}");
                    Console.WriteLine($"writer {name} failed: {e}");
                }
            }) { IsBackground = true, Name = $"b-shared-kill {name}" };
            thread.Start();
            return thread;
        }

        private static void Verify(StressContext ctx, string dir)
        {
            var writerFailures = Path.Combine(dir, "writer-failed.txt");
            if (File.Exists(writerFailures))
            {
                ctx.Fail($"a writer failed before the kill: {File.ReadAllText(writerFailures)}");
                return;
            }

            // journals on disk before the reopen are what recovery reads; a branch links the root's current journal after that
            var recoveredJournals = new Dictionary<string, HashSet<string>> { ["root"] = JournalNames(RootDir(dir)) };
            for (var b = 1; b <= Branches; b++)
                recoveredJournals[BranchName(b)] = JournalNames(BranchDir(dir, b));

            try
            {
                using var root = new StorageEnvironment(CreateOptions(RootDir(dir), rootJournal: null, prewarm: false));
                using var scope = root.Journal.SharedJournalsScope();
                using var merger = new Merger(root);

                var branches = new List<StorageEnvironment>();
                try
                {
                    for (var b = 1; b <= Branches; b++)
                        branches.Add(new StorageEnvironment(CreateOptions(BranchDir(dir, b), root.Journal, prewarm: false)));

                    long total = 0;
                    if (VerifyData(ctx, "root", root, RootDir(dir), ref total) == false)
                        return;
                    for (var b = 1; b <= Branches; b++)
                    {
                        if (VerifyData(ctx, BranchName(b), branches[b - 1], BranchDir(dir, b), ref total) == false)
                            return;
                    }

                    Console.WriteLine($"  iter {ctx.Iteration}: verified {total} committed txs across root and {Branches} branches");
                    if (total == 0)
                        ctx.VacuousIterations++;

                    // a recovered hard link is shared with other environments, recovery must leave it read-only
                    // (it may stay CurrentFile, the next write then rolls to a new journal)
                    var envs = new List<(string Name, StorageEnvironment Env)> { ("root", root) };
                    envs.AddRange(branches.Select((env, i) => (BranchName(i + 1), env)));
                    foreach (var (name, env) in envs)
                    {
                        foreach (var file in env.Journal.Files.Where(f => f.IsHardLinked && recoveredJournals[name].Contains(StorageEnvironmentOptions.JournalName(f.Number))))
                        {
                            if (file.DoneWriting?.IsRaised() != true)
                            {
                                ctx.Fail($"{name}: recovered hard-linked journal {file.Number} is writable after recovery " +
                                         $"(DoneWriting {(file.DoneWriting == null ? "null" : "not raised")}, current file {env.Journal.CurrentFile?.Number}, " +
                                         $"journals before the reopen: {string.Join(", ", recoveredJournals[name])})");
                                return;
                            }
                        }
                    }

                    for (var b = 1; b <= Branches; b++)
                    {
                        // a branch hands its old journals over to the root, it never keeps a pool of its own
                        var branchJournals = Path.Combine(BranchDir(dir, b), "Journals");
                        var kept = Directory.Exists(branchJournals) ? Directory.GetFiles(branchJournals, "recyclable-journal.*") : [];
                        if (kept.Length > 0)
                        {
                            ctx.Fail($"{BranchName(b)} keeps reusable journal files: {string.Join(", ", kept.Select(Path.GetFileName))}");
                            return;
                        }
                    }

                    // at startup the pool gathers every reusable file it can see, an untracked file is an orphan
                    var rootJournals = Path.Combine(RootDir(dir), "Journals");
                    var onDisk = Directory.GetFiles(rootJournals, "recyclable-journal.*").Length;
                    var tracked = root.Options.GetNumberOfJournalsForReuse();
                    if (onDisk != tracked)
                        ctx.Fail($"root pool mismatch after recovery: {onDisk} recyclable-journal files on disk, the pool tracks {tracked}");
                }
                finally
                {
                    foreach (var branch in branches)
                        branch.Dispose();
                }
            }
            catch (Exception e)
            {
                ctx.Fail($"recovery threw: {e}");
            }
        }

        private static bool VerifyData(StressContext ctx, string name, StorageEnvironment env, string envDir, ref long total)
        {
            long shadowCommitted = 0;
            var shadowPath = ShadowPath(envDir);
            if (File.Exists(shadowPath))
            {
                var bytes = File.ReadAllBytes(shadowPath);
                if (bytes.Length >= sizeof(long))
                    shadowCommitted = BitConverter.ToInt64(bytes);
            }

            long committed = 0;
            using var tx = env.ReadTransaction();
            var read = tx.ReadTree("meta")?.Read("count");
            if (read != null)
                committed = read.Reader.Read<long>();

            if (committed < shadowCommitted)
            {
                ctx.Fail($"{name}: durability loss: the shadow log acknowledges {shadowCommitted} committed transactions, recovery found {committed}");
                return false;
            }

            var tree = tx.ReadTree("data");
            if (committed > 0 && tree == null)
            {
                ctx.Fail($"{name}: the data tree is missing although {committed} transactions committed");
                return false;
            }

            for (long n = 1; n <= committed * ItemsPerTx; n++)
            {
                var result = tree.Read(KeyOf(n));
                if (result == null)
                {
                    ctx.Fail($"{name}: key {KeyOf(n)} is missing after recovery (committed count {committed})");
                    return false;
                }

                if (result.Reader.AsSpan().SequenceEqual(ValueOf(name, n)) == false)
                {
                    ctx.Fail($"{name}: key {KeyOf(n)} recovered with wrong content");
                    return false;
                }
            }

            total += committed;
            return true;
        }

        // what SharedIndexJournals does on the server: a root transaction writes the branch commits waiting to be merged
        private sealed class Merger : IJournalMerger, IDisposable
        {
            private readonly StorageEnvironment _root;
            private readonly ManualResetEventSlim _signal = new(false);
            private readonly CancellationTokenSource _cts = new();
            private readonly Thread _thread;

            public Merger(StorageEnvironment root)
            {
                _root = root;
                root.Journal.BranchJournalMerger = this;
                _thread = new Thread(Loop) { IsBackground = true, Name = "b-shared-kill merger" };
                _thread.Start();
            }

            public bool IsIdle => true;

            public void JournalMergeSubmitted() => _signal.Set();

            private void Loop()
            {
                while (_cts.IsCancellationRequested == false)
                {
                    // bounded wait: a signal can be consumed and reset before we look at it (RavenDB-26610)
                    if (_signal.Wait(100) == false && _root.Journal.HasBranchCommits == false)
                        continue;
                    _signal.Reset();

                    do
                    {
                        var current = _root.Journal.CurrentFile;
                        using (var tx = _root.WriteTransaction())
                            tx.Commit();

                        if (current == _root.Journal.CurrentFile)
                            continue;

                        // after a roll, a real change lets the root flush and release the older journals
                        using (var tx = _root.WriteTransaction())
                        {
                            tx.LowLevelTransaction.ModifyPage(0);
                            tx.Commit();
                        }
                    } while (_root.Journal.HasBranchCommits);
                }
            }

            public void Dispose()
            {
                _cts.Cancel();
                _thread.Join();
                _signal.Dispose();
                _cts.Dispose();
            }
        }

        private static StorageEnvironmentOptions CreateOptions(string dir, WriteAheadJournal rootJournal, bool prewarm)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.MaxLogFileSize = 64 * 1024;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxNumberOfRecyclableJournals = 3;
            options.EnableJournalPoolPrewarming = prewarm;
            options.RootJournal = rootJournal;
            options.OnRecoveryError += (_, _) => { }; // the server always subscribes, a torn tail after a kill is expected
            return options;
        }

        private static HashSet<string> JournalNames(string envDir)
        {
            var journals = Path.Combine(envDir, "Journals");
            return Directory.Exists(journals) ? Directory.GetFiles(journals, "*.journal").Select(Path.GetFileName).ToHashSet() : [];
        }

        private static string RootDir(string dir) => Path.Combine(dir, "root");

        private static string BranchDir(string dir, int branch) => Path.Combine(dir, BranchName(branch));

        private static string BranchName(int branch) => $"branch-{branch}";

        private static string ShadowPath(string envDir)
        {
            Directory.CreateDirectory(envDir);
            return Path.Combine(envDir, "shadow.bin");
        }

        private static string KeyOf(long n) => $"k/{n:D10}";

        private static byte[] ValueOf(string env, long n)
        {
            var value = new byte[512];
            var stamp = Encoding.ASCII.GetBytes($"{env}-value-{n}-");
            for (var i = 0; i < value.Length; i++)
                value[i] = stamp[i % stamp.Length];
            return value;
        }
    }
}
