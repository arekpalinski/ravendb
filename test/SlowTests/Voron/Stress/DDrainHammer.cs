using System;
using System.Collections.Concurrent;
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
    /// d-drain-hammer: several environments on the SAME device, which means they all share one
    /// DeviceWriteBudget instance and therefore one drain/trickle decision, one queue-depth sample and
    /// one sync-cost moving average. Each environment runs the real flusher and syncer plus threads
    /// forcing explicit flushes and syncs, and the write mix is built to swing the shared signals
    /// around: one environment does nothing but tiny transactions, one does multi-megabyte ones, and
    /// they take turns going completely quiet for long enough to expire the moving averages.
    ///
    /// Oracles: nothing throws, nothing deadlocks (every worker must keep making progress), the
    /// unsynced backlog DRAINS to zero within 120s once the writers stop, and after everything is
    /// disposed each environment reopens holding exactly the keys whose commits returned.
    /// Deliberately no absolute bound on the backlog while writing - see finding s-12, a writer faster
    /// than fsync builds one on this branch and on the merge base alike.
    /// </summary>
    public static class DDrainHammer
    {
        // VORON_DRAIN_ENVS=1 runs the big-transaction writer on its own, which is the control that
        // separates "the shared syncer is not getting to me" from "this writer simply outruns the disk"
        private static readonly int Environments =
            int.TryParse(Environment.GetEnvironmentVariable("VORON_DRAIN_ENVS"), out var n) ? n : 4;

        // VORON_DRAIN_HAMMER=0 drops the explicit flush/sync threads, leaving only the writers and the
        // real flusher/syncer - the production-shaped control
        private static readonly bool Hammer =
            Environment.GetEnvironmentVariable("VORON_DRAIN_HAMMER") != "0";

        // VORON_DRAIN_WRITEBACK=0 sets SyncWritebackBlockSizeInMb to 0, which takes ShouldSyncNow's
        // deferral branch out and makes it sync above MaxUnsyncedBytesBeforeSync instead of waiting for
        // the mandatory ceiling
        private static readonly bool Writeback =
            Environment.GetEnvironmentVariable("VORON_DRAIN_WRITEBACK") != "0";

        public static int Run(StressContext ctx)
        {
            ctx.Iterations = 1;
            var root = ctx.IterationDir(0);
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
            Directory.CreateDirectory(root);

            var dirs = Enumerable.Range(0, Environments).Select(i => Path.Combine(root, $"env-{i}")).ToArray();
            var envs = new StorageEnvironment[Environments];
            var committed = new ConcurrentDictionary<string, int>[Environments];
            var progress = new long[Environments];

            for (var i = 0; i < Environments; i++)
            {
                Directory.CreateDirectory(dirs[i]);
                envs[i] = new StorageEnvironment(CreateOptions(dirs[i]));
                committed[i] = new ConcurrentDictionary<string, int>();
            }

            var sp = Stopwatch.StartNew();
            using var stop = new CancellationTokenSource(TimeSpan.FromMinutes(ctx.Minutes * 0.8));
            var token = stop.Token;
            var threads = new List<Thread>();

            for (var i = 0; i < Environments; i++)
            {
                var index = i;
                var env = envs[i];

                // env 0: tiny transactions, never quiet. env 1: multi-MB transactions.
                // env 2 and 3: alternate between bursts and long silences, so the shared moving
                // averages expire and have to be rebuilt while the others are still hammering
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(ctx.Seed + index);
                        var n = 0;
                        while (token.IsCancellationRequested == false)
                        {
                            var profile = Environments == 1 ? 1 : index;
                            var size = profile switch
                            {
                                0 => 64,
                                1 => 512 * Constants.Size.Kilobyte + rng.Next(Constants.Size.Megabyte),
                                _ => 4 * Constants.Size.Kilobyte
                            };

                            var value = new byte[size];
                            var batch = profile == 1 ? 1 : 8;
                            for (var b = 0; b < batch; b++)
                            {
                                var key = $"k/{n++:D8}";
                                var stamp = n & 0xFF;
                                Array.Fill(value, (byte)stamp);

                                using var tx = env.WriteTransaction();
                                tx.CreateTree("items").Add(key, value);
                                tx.Commit();

                                committed[index][key] = stamp;
                            }

                            Interlocked.Increment(ref progress[index]);

                            if (profile >= 2 && n % 200 == 0)
                                Thread.Sleep(8000); // long enough to expire the queue-depth and sync-cost windows
                        }
                    }
                    catch (Exception e)
                    {
                        ctx.Fail($"env {index} writer threw {e}");
                        stop.Cancel();
                    }
                }));

                if (Hammer == false)
                    continue;

                // explicit flush + sync hammering, concurrently with the real flusher and syncer
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(ctx.Seed ^ (0x5150 + index));
                        while (token.IsCancellationRequested == false)
                        {
                            Thread.Sleep(rng.Next(200));
                            // the real flusher is running; this just piles extra requests on it
                            GlobalFlushingBehavior.GlobalFlusher.Value.ForTestingPurposesOnly().AddEnvironmentToFlushQueue(env);
                            if (rng.Next(3) == 0)
                                env.SyncDataFileImmediately();
                        }
                    }
                    catch (Exception e)
                    {
                        ctx.Fail($"env {index} flush/sync hammer threw {e}");
                        stop.Cancel();
                    }
                }));
            }

            var watchdog = new Thread(() =>
            {
                try
                {
                    var last = new long[Environments];
                    var lastMoved = new TimeSpan[Environments];
                    var peakUnsynced = new long[Environments];
                    var lastReport = -1;

                    while (token.IsCancellationRequested == false)
                    {
                        Thread.Sleep(2000);
                        for (var i = 0; i < Environments; i++)
                        {
                            var now = Interlocked.Read(ref progress[i]);
                            if (now > last[i])
                            {
                                last[i] = now;
                                lastMoved[i] = sp.Elapsed;
                            }
                            // env 2 and 3 sleep 8s on purpose, so the bar has to clear that
                            else if ((sp.Elapsed - lastMoved[i]).TotalSeconds > 60)
                            {
                                ctx.Fail($"env {i} made no progress for 60s while the other environments kept writing to the same device - deadlock or starvation on the shared write budget");
                                stop.Cancel();
                                return;
                            }

                            // no absolute bound here on purpose: a writer faster than fsync builds an
                            // unsynced backlog on this branch and on the merge base alike (finding s-12),
                            // so the meaningful property is that it drains once the writers stop, which
                            // is checked after the join
                            var unsynced = envs[i].Journal.Applicator.TotalWrittenButUnsyncedBytes;
                            peakUnsynced[i] = Math.Max(peakUnsynced[i], unsynced);
                        }

                        var tick = (int)(sp.Elapsed.TotalSeconds / 30);
                        if (tick != lastReport)
                        {
                            lastReport = tick;
                            var current = envs.Select(e => e.Journal.Applicator.TotalWrittenButUnsyncedBytes / Constants.Size.Megabyte);
                            Console.WriteLine($"  t+{sp.Elapsed.TotalSeconds:0}s: batches {string.Join("/", last)}, unsynced MB now {string.Join("/", current)} peak {string.Join("/", peakUnsynced.Select(x => x / Constants.Size.Megabyte))}");
                        }
                    }
                }
                catch (Exception e)
                {
                    ctx.Fail($"watchdog threw {e}");
                    stop.Cancel();
                }
            });

            foreach (var thread in threads)
                thread.Start();
            watchdog.Start();

            foreach (var thread in threads)
                thread.Join();
            watchdog.Join();

            // the writers have stopped: every environment must drain its unsynced backlog on its own
            var drain = Stopwatch.StartNew();
            while (drain.Elapsed.TotalSeconds < 120)
            {
                if (envs.All(e => e.Journal.Applicator.TotalWrittenButUnsyncedBytes == 0))
                    break;
                Thread.Sleep(1000);
            }

            var stillDirty = envs
                .Select((e, i) => (i, bytes: e.Journal.Applicator.TotalWrittenButUnsyncedBytes))
                .Where(x => x.bytes != 0)
                .ToArray();

            if (stillDirty.Length > 0 && ctx.Failures.Count == 0)
                ctx.Fail($"after the writers stopped, {stillDirty.Length} environments still had unsynced bytes after 120s: {string.Join(", ", stillDirty.Select(x => $"env {x.i} = {x.bytes / Constants.Size.Megabyte}MB"))}");
            else if (ctx.Failures.Count == 0)
                Console.WriteLine($"  drained in {drain.Elapsed.TotalSeconds:0}s after the writers stopped");

            for (var i = 0; i < Environments; i++)
                envs[i].Dispose();

            if (ctx.Failures.Count == 0)
            {
                for (var i = 0; i < Environments; i++)
                {
                    if (Verify(ctx, dirs[i], committed[i], i) == false)
                        break;
                }
            }

            if (ctx.Failures.Count == 0)
                Console.WriteLine($"  clean: {Environments} environments sharing one device write budget, {committed.Sum(c => c.Count)} keys all present after reopen");

            return ctx.Finish();
        }

        private static bool Verify(StressContext ctx, string dir, ConcurrentDictionary<string, int> committed, int index)
        {
            try
            {
                using var env = new StorageEnvironment(CreateOptions(dir));
                using var tx = env.ReadTransaction();
                var tree = tx.ReadTree("items");

                foreach (var (key, stamp) in committed)
                {
                    var read = tree?.Read(key);
                    if (read == null)
                    {
                        ctx.Fail($"env {index}: key {key} was committed but is missing after reopen ({committed.Count} keys expected)");
                        return false;
                    }

                    var b = new byte[1];
                    read.Reader.Read(b, 0, 1);
                    if (b[0] != stamp)
                    {
                        ctx.Fail($"env {index}: key {key} has stamp {b[0]}, expected {stamp}");
                        return false;
                    }
                }

                return true;
            }
            catch (Exception e)
            {
                ctx.Fail($"env {index}: reopen threw {e}");
                return false;
            }
        }

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = false; // the real flusher and syncer, that is the point
            options.ManualSyncing = false;
            options.MaxLogFileSize = 4 * Constants.Size.Megabyte;
            options.MaxUnsyncedBytesBeforeSync = 4 * Constants.Size.Megabyte;
            options.MaxUnsyncedBytesBeforeMandatorySync = 16 * Constants.Size.Megabyte;
            if (Writeback == false)
                options.SyncWritebackBlockSizeInMb = 0;
            return options;
        }
    }
}
