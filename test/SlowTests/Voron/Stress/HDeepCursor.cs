using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Voron;
using Voron.Data.BTrees;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// h-deep-cursor: trees deeper than TreeCursor's inline path (FoundTreePageDescriptor.MaxCursorPath
    /// = 8), so every cursor takes the overflow allocations - _overflowPath in the constructor,
    /// GrowUnlikely in PushCore and Slot() indexing past the inline array. Those are only reachable
    /// when the descent path is longer than 8 pages, which the comment on TreeCursor calls
    /// unreachable in practice; it is reachable in a few hundred entries once keys are near
    /// Constants.Tree.MaxKeySize, because a branch page then fits only two separators.
    ///
    /// Splits and rebalances are driven through those deep cursors while a model checks the full
    /// enumeration, point reads, seeks and backward iteration after every transaction. The scenario
    /// fails if the tree never gets deep enough, so a PASS always means the overflow path ran.
    /// </summary>
    public static unsafe class HDeepCursor
    {
        private const int KeySize = 3800;
        private const int KeySpace = 6000;

        public static int Run(StressContext ctx)
        {
            ctx.Iterations = 1;
            var dir = ctx.IterationDir(0);
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
            Directory.CreateDirectory(dir);

            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 8 * 1024 * 1024;

            var env = new StorageEnvironment(options);

            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("deep");
                tx.Commit();
            }

            var plain = new SortedDictionary<int, int>();

            var rng = new Random(ctx.Seed);
            var sp = Stopwatch.StartNew();
            var budget = TimeSpan.FromMinutes(ctx.Minutes);
            long txs = 0;
            var maxDepth = 0;
            long depthDrifts = 0;
            var lastReport = -1;

            while (sp.Elapsed < budget && ctx.Failures.Count == 0)
            {
                txs++;
                using (var tx = env.WriteTransaction())
                {
                    var a = tx.CreateTree("deep");

                    // grow well past depth 8, then carve chunks out so the rebalancer walks the same
                    // deep cursors back up, then refill
                    var grow = plain.Count < 1200 || rng.Next(3) != 0;
                    if (grow)
                    {
                        var n = 40 + rng.Next(120);
                        for (var i = 0; i < n; i++)
                        {
                            var key = rng.Next(KeySpace);
                            var salt = rng.Next();
                            a.Add(KeyOf(key), ValueOf(salt));
                            plain[key] = salt;
                        }
                    }
                    else
                    {
                        Drain(rng, a, plain);
                    }

                    var real = RealDepth(a);
                    maxDepth = Math.Max(maxDepth, real);
                    // the header's Depth counter drifts (finding s-8, pre-existing and report-only),
                    // so it is counted, not failed on - this scenario is about the cursor, not the counter
                    if (a.ReadHeader().Depth != real)
                        depthDrifts++;
                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    if (Check(ctx, tx.ReadTree("deep"), plain, "deep", txs) == false)
                        break;
                }

                if (txs % 40 == 0)
                    env.FlushLogToDataFile();

                var tick = (int)(sp.Elapsed.TotalSeconds / 30);
                if (tick != lastReport)
                {
                    lastReport = tick;
                    Console.WriteLine($"  t+{sp.Elapsed.TotalSeconds:0}s: {txs} txs, depth peak {maxDepth} (header drifted {depthDrifts}x), keys {plain.Count}");
                }
            }

            env.Dispose();

            if (ctx.Failures.Count == 0)
            {
                using var reopened = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(dir));
                using var tx = reopened.ReadTransaction();
                Check(ctx, tx.ReadTree("deep"), plain, "deep after reopen", txs);
            }

            if (maxDepth <= 8)
                ctx.Fail($"the tree never got deeper than {maxDepth}, so the TreeCursor overflow path never ran - this run proves nothing");
            else if (ctx.Failures.Count == 0)
                Console.WriteLine($"  clean: {txs} txs, depth peak {maxDepth} (> 8, so the cursor overflow path ran), {plain.Count} keys, {depthDrifts} header-Depth drifts");

            return ctx.Finish();
        }

        // the true number of levels, measured over every leaf - the header's Depth counter is only
        // bumped on root split / root collapse and is what this scenario is checking, so it cannot be
        // the thing we trust to say the cursor got deep
        private static int RealDepth(Tree tree)
        {
            var max = 0;
            Walk(tree, tree.ReadHeader().RootPageNumber, 1, ref max);
            return max;
        }

        private static void Walk(Tree tree, long pageNumber, int level, ref int max)
        {
            var page = tree.GetReadOnlyTreePage(pageNumber);
            if (page.IsBranch == false)
            {
                if (level > max)
                    max = level;
                return;
            }

            for (var i = 0; i < page.NumberOfEntries; i++)
                Walk(tree, page.GetNode(i)->PageNumber, level + 1, ref max);
        }

        private static void Drain(Random rng, Tree tree, SortedDictionary<int, int> model)
        {
            // a contiguous run, so whole leaves empty out and the merge walks up several levels
            var keys = model.Keys.ToArray();
            if (keys.Length == 0)
                return;

            var start = rng.Next(keys.Length);
            var take = Math.Min(keys.Length - start, 30 + rng.Next(150));
            for (var i = 0; i < take; i++)
            {
                var key = keys[start + i];
                tree.Delete(KeyOf(key));
                model.Remove(key);
            }
        }

        private static bool Check(StressContext ctx, Tree tree, SortedDictionary<int, int> model, string name, long tx)
        {
            var seen = new List<int>(model.Count);
            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        seen.Add(KeyFrom(it.CurrentKey.ToString()));
                    } while (it.MoveNext());
                }
            }

            var expected = model.Keys.ToList();
            if (seen.Count != expected.Count)
            {
                var missing = expected.Except(seen).Take(5);
                var extra = seen.Except(expected).Take(5);
                ctx.Fail($"tx {tx} {name}: forward enumeration has {seen.Count} keys, model has {expected.Count} (missing {string.Join(",", missing)}; extra {string.Join(",", extra)})");
                return false;
            }

            for (var i = 0; i < seen.Count; i++)
            {
                if (seen[i] != expected[i])
                {
                    ctx.Fail($"tx {tx} {name}: forward enumeration diverges at {i}: got {seen[i]}, expected {expected[i]}");
                    return false;
                }
            }

            // backward iteration takes the other branch of Search/SetLastSearchPosition
            var back = new List<int>(model.Count);
            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.AfterAllKeys))
                {
                    do
                    {
                        back.Add(KeyFrom(it.CurrentKey.ToString()));
                    } while (it.MovePrev());
                }
            }

            back.Reverse();
            if (back.Count != expected.Count)
            {
                ctx.Fail($"tx {tx} {name}: backward enumeration has {back.Count} keys, forward had {seen.Count}");
                return false;
            }

            for (var i = 0; i < back.Count; i++)
            {
                if (back[i] != expected[i])
                {
                    ctx.Fail($"tx {tx} {name}: backward enumeration diverges at {i}: got {back[i]}, expected {expected[i]}");
                    return false;
                }
            }

            // seeks land on the first key >= the probe, including probes for deleted keys
            for (var probe = 0; probe < 24; probe++)
            {
                var key = (int)((tx * 7919 + probe * 104729) % KeySpace);
                var want = expected.FirstOrDefault(k => k >= key, -1);

                using var it = tree.Iterate(prefetch: false);
                using (Slice.From(tree.Llt.Allocator, KeyOf(key), out Slice slice))
                {
                    var got = it.Seek(slice) ? KeyFrom(it.CurrentKey.ToString()) : -1;
                    if (got != want)
                    {
                        ctx.Fail($"tx {tx} {name}: Seek({key}) landed on {got}, expected {want}");
                        return false;
                    }
                }
            }

            return true;
        }

        // keys near Constants.Tree.MaxKeySize (4049 at an 8KB page): a branch page then holds two
        // separators, so the tree gets deeper than the cursor's inline path within a few hundred entries
        private static string KeyOf(int key)
        {
            var prefix = $"k/{key:D6}/";
            var sb = new StringBuilder(KeySize);
            sb.Append(prefix);
            while (sb.Length < KeySize)
                sb.Append('p');
            return sb.ToString(0, KeySize);
        }

        private static int KeyFrom(string key) => int.Parse(key.Substring(2, 6));

        private static string ValueOf(int salt) => salt.ToString("D12");
    }
}
