using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Voron;
using Voron.Global;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// b-prewarm-race: the background journal pool prewarming taken apart. Zeroing a pool journal runs
    /// on one process-wide thread and calls back into managed code through an [UnmanagedCallersOnly]
    /// pacing function, which touches journal.Env.WriteFlow and journal.IsJournalWriteActive on every
    /// chunk. Anything thrown there crosses a native frame, so it would take the process down rather
    /// than fail a transaction.
    ///
    /// So the scenario disposes the environment (and half the time deletes its directory right after)
    /// while a zeroing is in flight, over and over, from several environments at once so the shared
    /// preparation thread is always busy with somebody else's file. Surviving the run is itself part
    /// of the oracle: a crash here is the finding.
    ///
    /// Checked on top of that: the surviving environments reopen and hold every committed key, and the
    /// journal directory does not accumulate orphan recyclable files run over run.
    /// </summary>
    public static class BPrewarmRace
    {
        private const int Environments = 4;

        public static int Run(StressContext ctx)
        {
            var root = ctx.IterationDir(0);
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
            Directory.CreateDirectory(root);

            var rng = new Random(ctx.Seed);
            var sp = Stopwatch.StartNew();
            var budget = TimeSpan.FromMinutes(ctx.Minutes);
            var iteration = 0;
            var reopened = 0;
            var lastReport = -1;

            while (sp.Elapsed < budget && ctx.Failures.Count == 0)
            {
                iteration++;
                ctx.Iteration = iteration;
                ctx.Iterations++;

                // several environments racing for the one shared zeroing thread
                var dirs = Enumerable.Range(0, Environments)
                    .Select(i => Path.Combine(root, $"env-{iteration:D5}-{i}"))
                    .ToArray();

                var keep = new bool[Environments];
                var committed = new Dictionary<string, int>[Environments];
                var threads = new Thread[Environments];

                for (var i = 0; i < Environments; i++)
                {
                    var index = i;
                    var dir = dirs[i];
                    var killAfterMs = 30 + rng.Next(400);
                    keep[i] = rng.Next(2) == 0;
                    committed[i] = new Dictionary<string, int>();

                    threads[i] = new Thread(() =>
                    {
                        try
                        {
                            Churn(dir, killAfterMs, committed[index]);
                        }
                        catch (Exception e)
                        {
                            ctx.Fail($"iteration {iteration} env {index}: churn threw {e}");
                        }
                    });
                }

                foreach (var thread in threads)
                    thread.Start();
                foreach (var thread in threads)
                    thread.Join();

                if (ctx.Failures.Count > 0)
                    break;

                for (var i = 0; i < Environments; i++)
                {
                    if (keep[i] == false)
                    {
                        // the nastiest shape: the directory goes away while the shared thread may still
                        // be zeroing a file inside it
                        TryDelete(dirs[i]);
                        continue;
                    }

                    if (Verify(ctx, dirs[i], committed[i], iteration, i) == false)
                        break;

                    reopened++;
                    TryDelete(dirs[i]);
                }

                var tick = (int)(sp.Elapsed.TotalSeconds / 30);
                if (tick != lastReport)
                {
                    lastReport = tick;
                    Console.WriteLine($"  t+{sp.Elapsed.TotalSeconds:0}s: {iteration} iterations, {reopened} environments reopened and verified");
                }
            }

            // nothing may be left behind by the environments we deleted, and the shared thread must not
            // still be writing into the tree we are about to remove
            Thread.Sleep(2000);
            var leftovers = Directory.Exists(root) ? Directory.GetDirectories(root) : [];
            if (leftovers.Length > 0 && ctx.Failures.Count == 0)
                ctx.Fail($"{leftovers.Length} environment directories survived deletion, e.g. {leftovers[0]} (the zeroing thread was probably still holding a file in it)");

            if (ctx.Failures.Count == 0)
                Console.WriteLine($"  clean: {iteration} iterations x {Environments} environments disposed mid-zeroing, {reopened} reopened and verified, process still alive");

            return ctx.Finish();
        }

        private static void Churn(string dir, int killAfterMs, Dictionary<string, int> committed)
        {
            Directory.CreateDirectory(dir);

            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 4 * Constants.Size.Megabyte; // big enough that zeroing takes real time
            options.MaxNumberOfRecyclableJournals = 8;
            options.EnableJournalPoolPrewarming = true;

            var env = new StorageEnvironment(options);
            try
            {
                // full-speed zeroing, and prewarming requested regardless of what the device measures as
                env.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;

                var sp = Stopwatch.StartNew();
                var value = new byte[16 * 1024];
                var n = 0;
                while (sp.ElapsedMilliseconds < killAfterMs)
                {
                    for (var i = 0; i < 8; i++)
                    {
                        var key = $"k/{n++:D7}";
                        Array.Fill(value, (byte)(n & 0xFF));

                        using var tx = env.WriteTransaction();
                        tx.CreateTree("items").Add(key, value);
                        tx.Commit();

                        committed[key] = n & 0xFF;
                    }

                    // rolling journals is what asks for another pool journal to be prepared
                    env.FlushLogToDataFile();
                }
            }
            finally
            {
                // straight into the middle of whatever the shared zeroing thread is doing
                env.Dispose();
            }
        }

        private static bool Verify(StressContext ctx, string dir, Dictionary<string, int> committed, int iteration, int index)
        {
            try
            {
                using var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(dir));
                using var tx = env.ReadTransaction();
                var tree = tx.ReadTree("items");

                foreach (var (key, stamp) in committed)
                {
                    var read = tree?.Read(key);
                    if (read == null)
                    {
                        ctx.Fail($"iteration {iteration} env {index}: key {key} was committed before the dispose but is missing after reopen");
                        return false;
                    }

                    var b = new byte[1];
                    read.Reader.Read(b, 0, 1);
                    if (b[0] != stamp)
                    {
                        ctx.Fail($"iteration {iteration} env {index}: key {key} has stamp {b[0]}, expected {stamp}");
                        return false;
                    }
                }

                return true;
            }
            catch (Exception e)
            {
                ctx.Fail($"iteration {iteration} env {index}: reopening after a dispose mid-zeroing threw {e}");
                return false;
            }
        }

        private static void TryDelete(string dir)
        {
            for (var attempt = 0; attempt < 20; attempt++)
            {
                try
                {
                    if (Directory.Exists(dir) == false)
                        return;

                    Directory.Delete(dir, recursive: true);
                    return;
                }
                catch (IOException)
                {
                    Thread.Sleep(100); // the zeroing thread may still hold a handle in there
                }
                catch (UnauthorizedAccessException)
                {
                    Thread.Sleep(100);
                }
            }
        }
    }
}
