using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Fixed;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// e-parent-churn: many FixedSizeTrees under ONE parent tree, with the parent tree itself churned
    /// hard between FST operations (large variable-size values added/removed under the same parent so
    /// its pages split, merge and move), plus periodic drop-and-recreate of FSTs (instance repurposing).
    /// The two per-instance FST caches - the raw pointer to the FST header inside the parent page
    /// (guarded by Tree.StructureVersion) and the append fast-path (rightmost leaf + max key) - must
    /// stay correct across all of it. In-process, no kill: after every batch each FST's full
    /// enumeration and NumberOfEntries are compared to a model, and a restart at the end re-verifies.
    /// Runs in Debug so the FST/tree validation asserts are live.
    /// </summary>
    public static class EParentChurn
    {
        private const int TreeCount = 24;
        private const byte ValueSize = sizeof(long);
        private const int BatchesPerIteration = 40;

        public static int Run(StressContext ctx)
        {
            for (var iteration = 0; ctx.TimeLeft; iteration++)
            {
                ctx.Iteration = iteration;
                ctx.Iterations++;
                var dir = ctx.IterationDir(iteration);
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                Directory.CreateDirectory(dir);

                try
                {
                    RunIteration(ctx, dir, unchecked(ctx.Seed * 7919 + iteration));
                }
                catch (Exception e)
                {
                    ctx.Fail($"iteration {iteration} threw: {e}");
                }

                if (ctx.Failures.Count > 0)
                    break;

                try { Directory.Delete(dir, recursive: true); } catch { }
            }

            return ctx.Finish();
        }

        private static void RunIteration(StressContext ctx, string dir, int seed)
        {
            var rng = new Random(seed);
            var models = new SortedSet<long>[TreeCount];
            var maxKey = new long[TreeCount];
            for (var i = 0; i < TreeCount; i++)
                models[i] = new SortedSet<long>();

            long parentCounter = 0;
            var parentLive = new List<long>();

            using (var env = new StorageEnvironment(CreateOptions(dir)))
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                for (var batch = 0; batch < BatchesPerIteration; batch++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var parent = tx.CreateTree("parent");

                        // interleave: FST ops <-> parent-tree churn, several rounds per tx
                        var rounds = 2 + rng.Next(6);
                        for (var r = 0; r < rounds; r++)
                        {
                            // FST work on 1-4 trees
                            var trees = 1 + rng.Next(4);
                            for (var t = 0; t < trees; t++)
                            {
                                var idx = rng.Next(TreeCount);
                                using (Slice.From(allocator, TreeName(idx), out var name))
                                {
                                    var fst = parent.FixedTreeFor(name, ValueSize);
                                    var ops = 5 + rng.Next(40);
                                    for (var o = 0; o < ops; o++)
                                    {
                                        var roll = rng.Next(100);
                                        if (roll < 55)
                                        {
                                            var key = ++maxKey[idx]; // monotonic append - fast-path hot
                                            fst.Add(key, key * 397);
                                            models[idx].Add(key);
                                        }
                                        else if (roll < 75 && maxKey[idx] > 0)
                                        {
                                            var key = 1 + rng.NextInt64(maxKey[idx]);
                                            fst.Add(key, key * 397); // random insert / update
                                            models[idx].Add(key);
                                        }
                                        else if (models[idx].Count > 0)
                                        {
                                            var key = models[idx].ElementAt(rng.Next(Math.Min(models[idx].Count, 64)));
                                            fst.Delete(key);
                                            models[idx].Remove(key);
                                        }
                                    }
                                }
                            }

                            // parent-tree churn: values sized to force splits/moves of the parent's pages, which
                            // is exactly what relocates the FST headers the instances hold raw pointers to
                            var churn = 3 + rng.Next(12);
                            for (var c = 0; c < churn; c++)
                            {
                                if (rng.Next(3) == 0 && parentLive.Count > 0)
                                {
                                    var victim = parentLive[rng.Next(parentLive.Count)];
                                    parent.Delete(ParentKey(victim));
                                    parentLive.Remove(victim);
                                }
                                else
                                {
                                    var k = ++parentCounter;
                                    parent.Add(ParentKey(k), new string((char)('a' + k % 26), 100 + rng.Next(1500)));
                                    parentLive.Add(k);
                                }
                            }

                            // occasionally drop an FST entirely and recreate it (instance repurposing across trees)
                            if (rng.Next(25) == 0)
                            {
                                var idx = rng.Next(TreeCount);
                                using (Slice.From(allocator, TreeName(idx), out var name))
                                {
                                    parent.DeleteFixedTreeFor(name, ValueSize);
                                    models[idx].Clear();
                                    maxKey[idx] = 0;
                                    // start it again right away, in the same tx, with a fresh append run
                                    var fst = parent.FixedTreeFor(name, ValueSize);
                                    for (var o = 0; o < 20; o++)
                                    {
                                        var key = ++maxKey[idx];
                                        fst.Add(key, key * 397);
                                        models[idx].Add(key);
                                    }
                                }
                            }
                        }

                        tx.Commit();
                    }

                    if (batch % 8 == 7)
                        env.FlushLogToDataFile();

                    if (Verify(ctx, env, allocator, models, $"iteration {ctx.Iteration} batch {batch}") == false)
                        return;
                }
            }

            using (var env = new StorageEnvironment(CreateOptions(dir)))
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                if (Verify(ctx, env, allocator, models, $"iteration {ctx.Iteration} after restart") == false)
                    return;
            }

            Console.WriteLine($"  iter {ctx.Iteration}: {BatchesPerIteration} batches, {models.Sum(m => m.Count)} live FST keys across {TreeCount} trees, parent live {parentLive.Count}, verified after every batch + restart");
        }

        private static unsafe bool Verify(StressContext ctx, StorageEnvironment env, ByteStringContext allocator, SortedSet<long>[] models, string stage)
        {
            using var tx = env.ReadTransaction();
            var parent = tx.ReadTree("parent");
            for (var i = 0; i < TreeCount; i++)
            {
                using (Slice.From(allocator, TreeName(i), out var name))
                {
                    var fst = parent.FixedTreeFor(name, ValueSize);
                    var model = models[i];
                    if (fst.NumberOfEntries != model.Count)
                    {
                        ctx.Fail($"{stage}: {TreeName(i)} NumberOfEntries {fst.NumberOfEntries} != model {model.Count}");
                        return false;
                    }

                    var actual = new List<long>(model.Count);
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

                    if (actual.SequenceEqual(model) == false)
                    {
                        var missing = model.Except(actual).Take(5);
                        var extra = actual.Except(model).Take(5);
                        ctx.Fail($"{stage}: {TreeName(i)} key set diverged (missing {string.Join(",", missing)}; unexpected {string.Join(",", extra)})");
                        return false;
                    }

                    foreach (var key in model.Take(50).Concat(model.Reverse().Take(50)))
                    {
                        var ptr = fst.ReadPtr(key, out var size);
                        if (ptr == null || size != ValueSize || *(long*)ptr != key * 397)
                        {
                            ctx.Fail($"{stage}: {TreeName(i)} key {key} has a wrong or missing value");
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        private static string TreeName(int i) => $"fst/{i:D2}";

        private static string ParentKey(long k) => $"p/{k:D9}";

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.EnableJournalPoolPrewarming = false;
            return options;
        }
    }
}
