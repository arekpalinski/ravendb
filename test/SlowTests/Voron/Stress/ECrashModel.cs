using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Fixed;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// e-crash-model: several FixedSizeTrees under randomized op streams (monotonic appends, random
    /// inserts, deletes, re-adds, occasional full drains), committing every transaction, with the
    /// child process killed at seeded random commits. The op stream is a pure function of
    /// (seed, tx index, model state), so the parent replays the first K committed transactions
    /// (K read from the recovered environment itself) into an in-memory model and compares the
    /// full enumeration + entry count of every tree.
    /// </summary>
    public static class ECrashModel
    {
        private const int TreeCount = 8;
        private const ushort ValueSize = sizeof(long);
        private const int TxsBetweenFlush = 16;
        private const int TxsBetweenSync = 64;

        private sealed class TreeModel
        {
            public readonly List<long> Keys = new();
            public readonly HashSet<long> Set = new();
            public long MaxKey;
            public long LastDeleted = -1;

            public void Add(long key)
            {
                if (Set.Add(key))
                    Keys.Add(key);
                if (key > MaxKey)
                    MaxKey = key;
            }

            public void Remove(long key)
            {
                if (Set.Remove(key) == false)
                    return;
                var idx = Keys.IndexOf(key);
                Keys[idx] = Keys[^1];
                Keys.RemoveAt(Keys.Count - 1);
            }
        }

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

                ctx.RunChildAndKill(iteration, rng, minLifetimeMs: 2000, maxLifetimeMs: 10000);
                Verify(ctx, dir);

                if (ctx.Failures.Count > 0)
                    break;

                Directory.Delete(dir, recursive: true);
            }

            return ctx.Finish();
        }

        private static int RunChild(StressContext ctx)
        {
            var dir = ctx.IterationDir(ctx.Iteration);
            using var env = new StorageEnvironment(CreateOptions(dir));
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);

            Console.WriteLine($"child up, pid {Environment.ProcessId}, dir {dir}");

            var models = NewModels();
            var deadline = Stopwatch.StartNew();
            long t = 0;
            while (deadline.Elapsed.TotalSeconds < 30)
            {
                t++;
                using (var tx = env.WriteTransaction())
                {
                    GenerateTx(ctx.Seed, t, models, (treeIndex, key, isDelete) =>
                    {
                        using (Slice.From(allocator, TreeName(treeIndex), out var name))
                        {
                            FixedSizeTree fst = tx.FixedTreeFor(name, ValueSize);
                            if (isDelete)
                                fst.Delete(key);
                            else
                                fst.Add(key, ValueOf(key));
                        }
                    });

                    tx.CreateTree("meta").Add("t", BitConverter.GetBytes(t));
                    tx.Commit();
                }

                if (t % TxsBetweenFlush == 0)
                    env.FlushLogToDataFile();
                if (t % TxsBetweenSync == 0)
                    env.SyncDataFileImmediately();
            }

            return 0;
        }

        private static unsafe void Verify(StressContext ctx, string dir)
        {
            try
            {
                using var env = new StorageEnvironment(CreateOptions(dir));
                using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);

                long committed = 0;
                using (var tx = env.ReadTransaction())
                {
                    var read = tx.ReadTree("meta")?.Read("t");
                    if (read != null)
                        committed = read.Reader.Read<long>();
                }

                // replay the exact op stream the child committed
                var models = NewModels();
                for (long t = 1; t <= committed; t++)
                    GenerateTx(ctx.Seed, t, models, (_, _, _) => { });

                Console.WriteLine($"  iter {ctx.Iteration}: verified {committed} committed txs, {models.Sum(m => m.Set.Count)} live keys across {TreeCount} trees");
                if (committed == 0)
                    ctx.VacuousIterations++;

                using (var tx = env.ReadTransaction())
                {
                    for (var i = 0; i < TreeCount; i++)
                    {
                        using (Slice.From(allocator, TreeName(i), out var name))
                        {
                            FixedSizeTree fst = tx.FixedTreeFor(name, ValueSize);
                            var model = models[i];

                            if (fst.NumberOfEntries != model.Set.Count)
                            {
                                ctx.Fail($"{TreeName(i)}: NumberOfEntries {fst.NumberOfEntries} != model {model.Set.Count} after {committed} committed txs");
                                return;
                            }

                            var expected = model.Keys.OrderBy(k => k).ToList();
                            var actual = new List<long>((int)fst.NumberOfEntries);
                            using (var it = fst.Iterate())
                            {
                                if (it.Seek(long.MinValue))
                                {
                                    do
                                    {
                                        actual.Add(it.CurrentKey);
                                    } while (it.MoveNext());
                                }
                            }

                            if (expected.SequenceEqual(actual) == false)
                            {
                                var missing = expected.Except(actual).Take(5).ToList();
                                var extra = actual.Except(expected).Take(5).ToList();
                                ctx.Fail($"{TreeName(i)}: key set diverged after {committed} txs (missing: {string.Join(",", missing)}; unexpected: {string.Join(",", extra)})");
                                return;
                            }

                            // sample the stored values - a wrong slot reuse shows up here
                            var sampler = new Random(ctx.Seed ^ i);
                            for (var s = 0; s < Math.Min(100, expected.Count); s++)
                            {
                                var key = expected[sampler.Next(expected.Count)];
                                var ptr = fst.ReadPtr(key, out var size);
                                if (ptr == null || size != ValueSize || *(long*)ptr != ValueOf(key))
                                {
                                    ctx.Fail($"{TreeName(i)}: key {key} has a wrong or missing value");
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                ctx.Fail($"recovery threw: {e}");
            }
        }

        // The op stream: a pure function of (seed, t, model state). Both the child (applying to the
        // trees) and the verifying parent (model only) run this exact code.
        private static void GenerateTx(int seed, long t, TreeModel[] models, Action<int, long, bool> apply)
        {
            var rng = new Random(unchecked(seed * 486187739 + (int)t * 1000003));
            var appendPhase = t % 10 < 6;

            var treesInTx = 1 + rng.Next(3);
            for (var j = 0; j < treesInTx; j++)
            {
                var treeIndex = rng.Next(TreeCount);
                var model = models[treeIndex];
                var ops = 5 + rng.Next(26);

                if (appendPhase == false && model.Keys.Count > 0 && rng.Next(200) == 0)
                {
                    // full drain
                    foreach (var key in model.Keys.ToArray())
                    {
                        apply(treeIndex, key, true);
                        model.Remove(key);
                    }
                    continue;
                }

                for (var k = 0; k < ops; k++)
                {
                    var roll = rng.Next(100);
                    if (appendPhase ? roll < 70 : roll < 15)
                    {
                        // monotonic append - keeps the append fast-path hot
                        var key = model.MaxKey + 1;
                        apply(treeIndex, key, false);
                        model.Add(key);
                    }
                    else if (roll < (appendPhase ? 85 : 25) && model.MaxKey > 0)
                    {
                        // random insert (or in-place update of an existing key)
                        var key = 1 + NextLong(rng, model.MaxKey);
                        apply(treeIndex, key, false);
                        model.Add(key);
                    }
                    else if (model.LastDeleted != -1 && roll < (appendPhase ? 90 : 40))
                    {
                        // resurrect the key we just deleted
                        var key = model.LastDeleted;
                        model.LastDeleted = -1;
                        apply(treeIndex, key, false);
                        model.Add(key);
                    }
                    else if (model.Keys.Count > 0)
                    {
                        // delete an existing key (delete-heavy phases tombstone most of a page)
                        var key = model.Keys[rng.Next(model.Keys.Count)];
                        apply(treeIndex, key, true);
                        model.Remove(key);
                        model.LastDeleted = key;
                    }
                }
            }
        }

        private static long NextLong(Random rng, long maxInclusive) =>
            maxInclusive <= int.MaxValue ? rng.Next((int)maxInclusive) : rng.NextInt64(maxInclusive);

        private static TreeModel[] NewModels()
        {
            var models = new TreeModel[TreeCount];
            for (var i = 0; i < models.Length; i++)
                models[i] = new TreeModel();
            return models;
        }

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.MaxLogFileSize = 256 * 1024; // small journals - recovery replays several
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            return options;
        }

        private static string TreeName(int i) => $"fst/{i}";

        private static long ValueOf(long key) => key * 397;
    }
}
