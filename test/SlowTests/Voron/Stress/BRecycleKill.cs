using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Voron;
using Voron.Impl.Journal;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// b-recycle-kill: journals roll and recycle constantly (tiny MaxLogFileSize, prewarming forced),
    /// a child process is killed at seeded random moments (inside sync, inside zeroing, right after a
    /// roll). After recovery: every acknowledged transaction must be present, and every journal /
    /// recyclable-journal file on disk must be accounted for by the in-memory pool.
    /// </summary>
    public static class BRecycleKill
    {
        private const int ItemsPerTx = 20;
        private const int TxsBetweenFlushSync = 10;
        private const int TxsBetweenShadowWrites = 25;

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
            using var env = new StorageEnvironment(CreateOptions(dir, prewarm: true));
            env.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;

            using var shadow = new FileStream(ShadowPath(dir), FileMode.Create, FileAccess.Write, FileShare.Read);

            Console.WriteLine($"child up, pid {Environment.ProcessId}, dir {dir}");

            var deadline = Stopwatch.StartNew();
            long committed = 0;
            while (deadline.Elapsed.TotalSeconds < 30)
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("data");
                    for (var i = 0; i < ItemsPerTx; i++)
                    {
                        var n = committed * ItemsPerTx + i + 1;
                        tree.Add(KeyOf(n), ValueOf(n));
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

            return 0;
        }

        private static void Verify(StressContext ctx, string dir)
        {
            long shadowCommitted = 0;
            var shadowPath = ShadowPath(dir);
            if (File.Exists(shadowPath))
            {
                var bytes = File.ReadAllBytes(shadowPath);
                if (bytes.Length >= sizeof(long))
                    shadowCommitted = BitConverter.ToInt64(bytes);
            }

            try
            {
                using var env = new StorageEnvironment(CreateOptions(dir, prewarm: false));

                long committed = 0;
                using (var tx = env.ReadTransaction())
                {
                    var meta = tx.ReadTree("meta");
                    var read = meta?.Read("count");
                    if (read != null)
                        committed = read.Reader.Read<long>();

                    if (committed < shadowCommitted)
                    {
                        ctx.Fail($"durability loss: the shadow log acknowledges {shadowCommitted} committed transactions, recovery found {committed}");
                        return;
                    }

                    var tree = tx.ReadTree("data");
                    if (committed > 0 && tree == null)
                    {
                        ctx.Fail($"the data tree is missing although {committed} transactions committed");
                        return;
                    }

                    for (long n = 1; n <= committed * ItemsPerTx; n++)
                    {
                        var result = tree.Read(KeyOf(n));
                        if (result == null)
                        {
                            ctx.Fail($"key {KeyOf(n)} is missing after recovery (committed count {committed})");
                            return;
                        }

                        var expected = ValueOf(n);
                        var actual = result.Reader.AsSpan();
                        if (actual.SequenceEqual(expected) == false)
                        {
                            ctx.Fail($"key {KeyOf(n)} recovered with wrong content");
                            return;
                        }
                    }
                }

                Console.WriteLine($"  iter {ctx.Iteration}: verified {committed} committed txs ({committed * ItemsPerTx} keys), shadow floor {shadowCommitted}");
                if (committed == 0)
                    ctx.VacuousIterations++;

                // pool-vs-disk reconciliation: at startup the pool re-gathers every recyclable file it
                // can see, so an on-disk file the pool does not track is an orphan by definition
                var journalsDir = Path.Combine(dir, "Journals");
                var recyclableOnDisk = Directory.GetFiles(journalsDir, "recyclable-journal.*").Length;
                var tracked = env.Options.GetNumberOfJournalsForReuse();
                if (recyclableOnDisk != tracked)
                {
                    ctx.Fail($"pool mismatch after recovery: {recyclableOnDisk} recyclable-journal files on disk, the pool tracks {tracked}");
                    return;
                }

                // a cleanup must be able to delete everything it tracks; anything left behind is untracked
                env.FlushLogToDataFile();
                using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                    sync.SyncDataFile();

                env.Options.TryCleanupRecycledJournals();

                var leftBehind = Directory.GetFiles(journalsDir, "recyclable-journal.*");
                if (leftBehind.Length > 0)
                {
                    ctx.Fail($"{leftBehind.Length} recyclable-journal file(s) survived TryCleanupRecycledJournals - untracked orphans: {string.Join(", ", leftBehind)}");
                }
            }
            catch (Exception e)
            {
                ctx.Fail($"recovery threw: {e}");
            }
        }

        private static StorageEnvironmentOptions CreateOptions(string dir, bool prewarm)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.MaxLogFileSize = 64 * 1024;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxNumberOfRecyclableJournals = 3;
            options.EnableJournalPoolPrewarming = prewarm;
            return options;
        }

        private static string ShadowPath(string dir) => Path.Combine(dir, "shadow.bin");

        private static string KeyOf(long n) => $"k/{n:D10}";

        private static byte[] ValueOf(long n)
        {
            var value = new byte[512];
            var stamp = Encoding.ASCII.GetBytes($"value-{n}-");
            for (var i = 0; i < value.Length; i++)
                value[i] = stamp[i % stamp.Length];
            return value;
        }
    }
}
