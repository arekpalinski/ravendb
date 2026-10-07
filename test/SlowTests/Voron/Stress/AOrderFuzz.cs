using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Voron;
using Voron.Impl;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// a-order-fuzz: async-commit chains with K=64 in-flight journal writes whose completions are
    /// delayed by seeded per-write random sleeps, so acks land in adversarial orders. Sync commits
    /// are interleaved between chains (they take the inline path against a busy pipeline tail).
    /// Oracles: the pipeline's own Debug.Asserts (dirty slots, watermark ordering), read-transaction
    /// id monotonicity observed concurrently, full data verification, and a clean restart per
    /// iteration.
    /// </summary>
    public static class AOrderFuzz
    {
        private const int PipelineDepth = 64;
        private const int InFlightWindow = 32;
        private const int TxsPerIteration = 1500;

        public static int Run(StressContext ctx)
        {
            for (var iteration = 0; ctx.TimeLeft; iteration++)
            {
                ctx.Iteration = iteration;
                ctx.Iterations++;
                var dir = ctx.IterationDir(iteration);
                Directory.CreateDirectory(dir);

                try
                {
                    RunIteration(ctx, dir, iteration);
                }
                catch (Exception e)
                {
                    ctx.Fail($"iteration threw: {e}");
                }

                if (ctx.Failures.Count > 0)
                    break;

                Directory.Delete(dir, recursive: true);
            }

            return ctx.Finish();
        }

        private static void RunIteration(StressContext ctx, string dir, int iteration)
        {
            var iterationSeed = unchecked(ctx.Seed * 31 + iteration);
            var rng = new Random(iterationSeed);
            long committedKeys;

            using (var env = new StorageEnvironment(CreateOptions(dir)))
            {
                var monotonicityViolated = 0L;
                var watcher = StartReadIdMonotonicityWatcher(env, out var stopWatcher, v => Interlocked.Exchange(ref monotonicityViolated, v));

                env.Options.ForTestingPurposesOnly().OnJournalWrite = (posBy4Kb, _) =>
                {
                    // deterministic per-position delay - completions come back out of order
                    var delay = (int)(unchecked((ulong)(posBy4Kb * 2654435761L + iterationSeed)) % 40);
                    if (delay > 0)
                        Thread.Sleep(delay);
                };

                committedKeys = DriveChains(env, rng);

                env.Options.ForTestingPurposesOnly().OnJournalWrite = null;
                stopWatcher();
                watcher.Join();

                if (Interlocked.Read(ref monotonicityViolated) != 0)
                {
                    ctx.Fail($"CurrentReadTransactionId moved backwards (observed at tx {monotonicityViolated})");
                    return;
                }

                VerifyKeys(ctx, env, committedKeys, "before restart");
                if (ctx.Failures.Count > 0)
                    return;
            }

            using (var env = new StorageEnvironment(CreateOptions(dir)))
            {
                VerifyKeys(ctx, env, committedKeys, "after restart");
            }

            Console.WriteLine($"  iter {iteration}: {committedKeys} keys through the chain, verified twice");
        }

        private static long DriveChains(StorageEnvironment env, Random rng)
        {
            long keyCounter = 0;
            var context = new TransactionPersistentContext(true);
            var remaining = TxsPerIteration;

            while (remaining > 0)
            {
                // occasionally a plain sync commit against whatever the pipeline still has in flight
                if (rng.Next(10) == 0)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        WriteBatch(tx, ref keyCounter, rng);
                        tx.Commit();
                    }

                    remaining--;
                    continue;
                }

                var chainLength = Math.Min(remaining, 20 + rng.Next(80));
                remaining -= chainLength;

                var inFlight = new Queue<Transaction>();
                var previous = env.WriteTransaction(context);
                WriteBatch(previous, ref keyCounter, rng);

                for (var i = 1; i < chainLength; i++)
                {
                    var current = previous.BeginAsyncCommitAndStartNewTransaction(context);
                    inFlight.Enqueue(previous);

                    WriteBatch(current, ref keyCounter, rng);

                    while (inFlight.Count >= InFlightWindow)
                        CompleteOldest(inFlight);

                    previous = current;
                }

                while (inFlight.Count > 0)
                    CompleteOldest(inFlight);

                previous.Commit();
                previous.Dispose();
            }

            return keyCounter;

            static void CompleteOldest(Queue<Transaction> inFlight)
            {
                var oldest = inFlight.Dequeue();
                oldest.EndAsyncCommit();
                oldest.Dispose();
            }
        }

        private static void WriteBatch(Transaction tx, ref long keyCounter, Random rng)
        {
            var tree = tx.CreateTree("data");
            var items = 1 + rng.Next(8);
            var valueSize = rng.Next(3) switch
            {
                0 => 64,
                1 => 1024,
                _ => 16 * 1024 // large enough to vary the batch size across the pipelining cap
            };

            for (var i = 0; i < items; i++)
            {
                var n = ++keyCounter;
                tree.Add(KeyOf(n), ValueOf(n, valueSize));
            }
        }

        private static Thread StartReadIdMonotonicityWatcher(StorageEnvironment env, out Action stop, Action<long> reportViolation)
        {
            var running = true;
            var thread = new Thread(() =>
            {
                long last = 0;
                while (Volatile.Read(ref running))
                {
                    var current = env.CurrentReadTransactionId;
                    if (current < last)
                    {
                        reportViolation(current);
                        return;
                    }

                    last = current;
                }
            })
            {
                IsBackground = true
            };
            thread.Start();
            stop = () => Volatile.Write(ref running, false);
            return thread;
        }

        private static void VerifyKeys(StressContext ctx, StorageEnvironment env, long committedKeys, string stage)
        {
            using (var tx = env.ReadTransaction())
            {
                var tree = tx.ReadTree("data");
                for (long n = 1; n <= committedKeys; n++)
                {
                    var read = tree?.Read(KeyOf(n));
                    if (read == null)
                    {
                        ctx.Fail($"key {KeyOf(n)} missing {stage} ({committedKeys} committed)");
                        return;
                    }
                }
            }
        }

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxConcurrentJournalWrites = PipelineDepth;
            options.PipelineJournalWritesAboveLatencyInTicks = 0; // always pipeline-eligible
            options.MaxLogFileSize = 4 * 1024 * 1024; // several rolls per iteration
            return options;
        }

        private static string KeyOf(long n) => $"t/{n:D10}";

        private static byte[] ValueOf(long n, int size)
        {
            var value = new byte[size];
            var stamp = Encoding.ASCII.GetBytes($"v{n}-");
            for (var i = 0; i < value.Length; i++)
                value[i] = stamp[i % stamp.Length];
            return value;
        }
    }
}
