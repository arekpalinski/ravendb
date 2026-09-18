using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

// Review probes for PR 23650. Not meant to be merged as-is.
public class RavenDB_27533_Probes(ITestOutputHelper output) : StorageTest(output)
{
    private const long Stride = 1L << 40;

    // Experiment 3: grow to leaves at depth 3 (Release only, ~65M sequential entries), then LIFO-delete the right
    // edge until the right-most depth-2 branch collapses into its branch sibling. Expect unequal leaf depths that
    // the split-time wrap never repairs, since no leaf ends up next to a branch.
    [RavenFact(RavenTestCategory.Voron)]
    public void LifoDeleteSweep_CollapsesBranchNextToBranch_FixDoesNotRepair()
    {
        const string name = "entries";
        const long stride = 1L << 34; // 2^63 / 2^34 = 537M keys before overflow
        long next = stride;
        const int batch = 2_000_000;
        var log = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "claude", "RavenDB_27533_lifo_probe.log");
        void Log(string line) { output.WriteLine(line); System.IO.File.AppendAllText(log, DateTime.Now.ToString("HH:mm:ss ") + line + Environment.NewLine); }

        int[] depths;
        while (true)
        {
            using (var tx = Env.WriteTransaction())
            {
                var lookup = tx.LookupFor<Int64LookupKey>(name);
                for (int i = 0; i < batch; i++) { lookup.Add(next, next); next += stride; }
                depths = LeafDepths(lookup);
                Log($"grown: entries={lookup.NumberOfEntries} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} depths={string.Join(",", depths)}");
                tx.Commit();
            }
            Assert.Single(depths);
            if (depths[0] >= 3) break;
            Assert.True(next < long.MaxValue - (long)batch * stride, "would overflow before reaching depth 3");
        }

        using (var tx = Env.WriteTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>(name);
            long deleted = 0;
            while (true)
            {
                next -= stride;
                Assert.True(lookup.TryRemove(next, out _));
                deleted++;
                if (deleted % 1000 == 0)
                {
                    depths = LeafDepths(lookup);
                    if (depths.Length > 1)
                        break;
                    if (deleted % 100_000 == 0)
                        Log($"deleted {deleted}: depths={string.Join(",", depths)} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages}");
                }
            }
            Log($"first imbalance after {deleted} LIFO deletes: depths={string.Join(",", depths)} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} rootChildren={string.Join(",", ChildFlags(lookup, lookup.State.RootPage))} rightRootChildChildren={ChildFlags(lookup, lookup.AllEntriesIn(lookup.State.RootPage)[^1].Item2).Length}");

            // keep going until the right-most root child (R2) has collapsed into a branch of leaves
            long moreDeleted = 0;
            while (true)
            {
                next -= stride;
                Assert.True(lookup.TryRemove(next, out _));
                moreDeleted++;
                if (moreDeleted % 10_000 == 0)
                {
                    var right = lookup.AllEntriesIn(lookup.State.RootPage)[^1].Item2;
                    var rightChildren = ChildFlags(lookup, right);
                    if (rightChildren.All(f => f == LookupPageFlags.Leaf))
                        break;
                    if (moreDeleted % 500_000 == 0)
                        Log($"+{moreDeleted} deletes: depths={string.Join(",", LeafDepths(lookup))} rightRootChild children={rightChildren.Length} ({rightChildren.Count(f => f == LookupPageFlags.Leaf)} leaves)");
                    Assert.True(moreDeleted < 20_000_000, "R2 did not collapse");
                }
            }
            depths = LeafDepths(lookup);
            Log($"R2 collapsed after +{moreDeleted} deletes: depths={string.Join(",", depths)} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} rootChildren={string.Join(",", ChildFlags(lookup, lookup.State.RootPage))}");

            for (int i = 0; i < 2_000_000; i++) { lookup.Add(next, next); next += stride; }
            Log($"after 2M appends: depths={string.Join(",", LeafDepths(lookup))} branches={lookup.State.BranchPages} rootChildren={string.Join(",", ChildFlags(lookup, lookup.State.RootPage))}");

            lookup.VerifyStructure();
            Assert.True(depths.Length > 1, "expected unequal leaf depths after branch collapse");
        }
    }

    // Experiment 3b: does the imbalance compound over append/LIFO-delete rounds?
    [RavenFact(RavenTestCategory.Voron)]
    public void LifoAppendDeleteRounds_DepthSpreadOverTime()
    {
        long next = Stride;
        var keys = new Stack<long>();
        int maxSpread = 0;

        using var tx = Env.WriteTransaction();
        var lookup = tx.LookupFor<Int64LookupKey>("entries");

        var rnd = new Random(27533);
        for (int round = 0; round < 400; round++)
        {
            int adds = rnd.Next(200, 3000);
            for (int i = 0; i < adds; i++) Add();
            int dels = rnd.Next(0, Math.Min(keys.Count, 3000));
            for (int i = 0; i < dels; i++) RemoveTop();

            var d = LeafDepths(lookup);
            int spread = d.Max() - d.Min();
            maxSpread = Math.Max(maxSpread, spread);
            if (round % 40 == 0 || spread > 1)
                output.WriteLine($"round {round}: entries={lookup.NumberOfEntries} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} depths={string.Join(",", d)}");
        }

        lookup.VerifyStructure();
        output.WriteLine($"max spread = {maxSpread}");

        void Add() { lookup.Add(next, next); keys.Push(next); next += Stride; }
        void RemoveTop() { var k = keys.Pop(); Assert.True(lookup.TryRemove(k, out _)); next = k; }
    }

    // Experiment 2: reporter's repro, all leaf depths checked at every checkpoint.
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(1L)]
    [InlineData(1L << 40)]
    public void RepeatedUpdates_AllLeavesStaySameDepth(long stride)
    {
        const int batchSize = 10_000;
        long maxEntries = stride == 1 ? 1_500_000 : 300_000;
        const string name = "entries";
        long inserted = 0;
        int maxDepth = 0;

        while (inserted < maxEntries)
        {
            using (var tx = Env.WriteTransaction())
            {
                var lookup = tx.LookupFor<Int64LookupKey>(name);
                long end = inserted + batchSize;
                for (long i = inserted; i < end; i++)
                {
                    long key = i * stride + 1;
                    lookup.Add(key, key * 1024 + 1);
                    for (int u = 0; u < 3; u++)
                    {
                        Assert.True(lookup.TryRemove(key, out long prev));
                        lookup.Add(key, prev);
                    }
                }
                inserted = end;
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var lookup = tx.LookupFor<Int64LookupKey>(name);
                var depths = LeafDepths(lookup);
                if (depths.Max() > maxDepth || inserted % 200_000 == 0)
                    output.WriteLine($"entries={inserted} depths={string.Join(",", depths)} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages}");
                maxDepth = Math.Max(maxDepth, depths.Max());
                Assert.Single(depths);
            }
        }

        using (var tx = Env.ReadTransaction())
        {
            var lookup = tx.LookupFor<Int64LookupKey>(name);
            Assert.True(lookup.TryGetValue(1, out long v));
            Assert.Equal(1 * 1024 + 1, v);
            lookup.VerifyStructure();
        }
    }

    // Experiment 4: seeded fuzz, Int64 keys, mixed random / LIFO / FIFO / delete-heavy phases, committed periodically.
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Fuzz_Int64(int seed)
    {
        var rnd = new Random(seed);
        var model = new SortedDictionary<long, long>();
        const string name = "entries";
        int maxSpread = 0;

        using (var tx = Env.WriteTransaction())
        {
            // pre-seed to Root=[B1, B2] so the wrap / collapse paths under the root are exercised
            var lookup = tx.LookupFor<Int64LookupKey>(name);
            for (long k = 1L << 40; lookup.State.BranchPages < 3; k += 1L << 40)
            {
                lookup.Add(k, k ^ 0x5bd1e995);
                model[k] = k ^ 0x5bd1e995;
            }
            output.WriteLine($"seeded: entries={model.Count} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages}");
            tx.Commit();
        }

        for (int batch = 0; batch < 60; batch++)
        {
            using (var tx = Env.WriteTransaction())
            {
                var lookup = tx.LookupFor<Int64LookupKey>(name);
                int mode = rnd.Next(4); // 0 random, 1 lifo-heavy, 2 fifo-heavy, 3 delete-heavy
                int ops = rnd.Next(2000, 20000);
                for (int i = 0; i < ops; i++)
                {
                    bool add = mode == 3 ? rnd.Next(10) < 3 : rnd.Next(10) < 6;
                    if (add || model.Count == 0)
                    {
                        long key = mode switch
                        {
                            1 => (model.Count == 0 ? 0 : model.Keys.Last()) + rnd.Next(1, 4) * (1L << 40),
                            2 => (model.Count == 0 ? 0 : model.Keys.First()) - rnd.Next(1, 4) * (1L << 40),
                            _ => rnd.NextInt64(long.MinValue / 4, long.MaxValue / 4),
                        };
                        if (model.ContainsKey(key)) continue;
                        lookup.Add(key, key ^ 0x5bd1e995);
                        model[key] = key ^ 0x5bd1e995;
                    }
                    else
                    {
                        long key = mode switch
                        {
                            1 => model.Keys.Last(),
                            2 => model.Keys.First(),
                            _ => model.Keys.ElementAt(rnd.Next(model.Count)),
                        };
                        Assert.True(lookup.TryRemove(key, out long v));
                        Assert.Equal(model[key], v);
                        model.Remove(key);
                    }
                }

                lookup.VerifyStructure();
                Assert.Equal(model.Count, lookup.NumberOfEntries);
                if (model.Count > 0)
                {
                    var d = LeafDepths(lookup);
                    maxSpread = Math.Max(maxSpread, d.Max() - d.Min());
                }
                tx.Commit();
            }

            if (batch % 10 == 9)
            {
                using var tx = Env.ReadTransaction();
                var lookup = tx.LookupFor<Int64LookupKey>(name);
                var it = lookup.Iterate();
                it.Reset();
                using var expected = model.GetEnumerator();
                long count = 0;
                while (it.MoveNext(out Int64LookupKey key, out long value, out _))
                {
                    Assert.True(expected.MoveNext());
                    Assert.Equal(expected.Current.Key, key.Value);
                    Assert.Equal(expected.Current.Value, value);
                    count++;
                }
                Assert.Equal(model.Count, count);
                foreach (var kv in model.Take(2000))
                {
                    Assert.True(lookup.TryGetValue(kv.Key, out long v));
                    Assert.Equal(kv.Value, v);
                }
                output.WriteLine($"batch {batch}: entries={model.Count} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} depths={string.Join(",", LeafDepths(lookup))}");
            }
        }
        output.WriteLine($"max depth spread = {maxSpread}");
    }

    // Experiment 4b: CompactTree (CompactKeyLookup) goes through the same SplitPage / WrapPageInBranch.
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(1)]
    [InlineData(2)]
    public void Fuzz_CompactTree(int seed)
    {
        var rnd = new Random(seed);
        var model = new SortedDictionary<string, long>(StringComparer.Ordinal);
        const string name = "terms";
        int maxSpread = 0;

        for (int batch = 0; batch < 40; batch++)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CompactTreeFor(name);
                int mode = rnd.Next(3); // 0 random, 1 lifo (ascending keys), 2 delete-heavy
                int ops = rnd.Next(2000, 15000);
                for (int i = 0; i < ops; i++)
                {
                    bool add = mode == 2 ? rnd.Next(10) < 3 : rnd.Next(10) < 6;
                    if (add || model.Count == 0)
                    {
                        string key = mode == 1
                            ? "term-" + (model.Count == 0 ? 0 : long.Parse(model.Keys.Last().Substring(5)) + rnd.Next(1, 4)).ToString("D12")
                            : "term-" + rnd.NextInt64(0, 1_000_000_000_000).ToString("D12");
                        if (model.ContainsKey(key)) continue;
                        long value = rnd.NextInt64(1, long.MaxValue / 2);
                        tree.Add(key, value);
                        model[key] = value;
                    }
                    else
                    {
                        string key = mode == 1 ? model.Keys.Last() : model.Keys.ElementAt(rnd.Next(model.Count));
                        Assert.True(tree.TryRemove(key, out long v));
                        Assert.Equal(model[key], v);
                        model.Remove(key);
                    }
                }

                tree._inner.VerifyStructure();
                Assert.Equal(model.Count, tree.NumberOfEntries);
                if (model.Count > 0)
                {
                    var d = LeafDepths(tree._inner);
                    maxSpread = Math.Max(maxSpread, d.Max() - d.Min());
                }
                tx.Commit();
            }

            if (batch % 10 == 9)
            {
                using var tx = Env.ReadTransaction();
                var tree = tx.CompactTreeFor(name);
                foreach (var kv in model.Take(3000))
                {
                    Assert.True(tree.TryGetValue(kv.Key, out long v), kv.Key);
                    Assert.Equal(kv.Value, v);
                }
                output.WriteLine($"batch {batch}: entries={model.Count} branches={tree.BranchPages} leaves={tree.LeafPages} depths={string.Join(",", LeafDepths(tree._inner))}");
            }
        }
        output.WriteLine($"max depth spread = {maxSpread}");
    }

    private static unsafe LookupPageFlags[] ChildFlags<T>(Lookup<T> lookup, long page) where T : struct, ILookupKey =>
        lookup.AllEntriesIn(page).Select(x => ((LookupPageHeader*)lookup.Llt.GetPage(x.Item2).Pointer)->PageFlags).ToArray();

    private static int[] LeafDepths<T>(Lookup<T> lookup) where T : struct, ILookupKey =>
        LeafDepths(lookup, lookup.State.RootPage, 0).Distinct().OrderBy(x => x).ToArray();

    private static unsafe IEnumerable<int> LeafDepths<T>(Lookup<T> lookup, long page, int depth) where T : struct, ILookupKey
    {
        var header = (LookupPageHeader*)lookup.Llt.GetPage(page).Pointer;
        if (header->IsLeaf)
            return new[] { depth };
        return lookup.AllEntriesIn(page).SelectMany(x => LeafDepths(lookup, x.Item2, depth + 1)).Distinct().ToList();
    }
}
