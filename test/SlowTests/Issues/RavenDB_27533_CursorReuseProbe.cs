using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

// Review probe for PR 23650: the entries-to-terms tracker (IndexWriter.EntriesToTermsTracker.CommitCurrentDataFor)
// reuses the internal cursor across sorted keys via TryGetNextValue + AddOrSetAfterGetNext / TryRemoveExistingValue
// with no structure-version check. Drive a wrap (leaf next to branch splits) and a collapse in the middle of such batches.
public unsafe class RavenDB_27533_CursorReuseProbe(ITestOutputHelper output) : StorageTest(output)
{
    private const long Stride = 1L << 40;

    [RavenFact(RavenTestCategory.Voron)]
    public void SortedBatchViaTryGetNextValue_AcrossWrapAndCollapse()
    {
        var model = new SortedDictionary<long, long>();
        long next = Stride;

        using var tx = Env.WriteTransaction();
        var lookup = tx.LookupFor<Int64LookupKey>("entries");

        // Root=[B1, B2], B2=[stolen full leaf, one-entry leaf]
        while (lookup.State.BranchPages < 3)
        {
            lookup.Add(next, next);
            model[next] = next;
            next += Stride;
        }
        output.WriteLine($"seeded entries={model.Count} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages}");

        var rnd = new Random(27533);
        for (int round = 0; round < 300; round++)
        {
            // 1) sorted removal batch through the cursor-reuse path, taken from the top (LIFO) so the collapse fires
            int removeCount = rnd.Next(1, 40);
            var toRemove = model.Keys.Reverse().Take(removeCount).OrderBy(x => x).ToArray();
            lookup.InitializeCursorState();
            foreach (var k in toRemove)
            {
                Int64LookupKey key = k;
                Assert.True(lookup.TryGetNextValue(ref key, out long found), $"round {round} key {k}");
                Assert.Equal(model[k], found);
                Assert.True(lookup.TryRemoveExistingValue(ref key, out long removed));
                Assert.Equal(model[k], removed);
                model.Remove(k);
            }
            next = model.Count == 0 ? Stride : model.Keys.Last() + Stride;

            // 2) sorted addition batch through the cursor-reuse path, some keys interleaved into the tail, most appended
            int addCount = rnd.Next(1, 400);
            var toAdd = new List<long>();
            for (int i = 0; i < addCount; i++) { toAdd.Add(next); next += Stride; }
            // also touch a few existing keys (update path) and a few deep-in-the-middle keys (cursor has to climb)
            foreach (var k in model.Keys.Reverse().Take(3)) toAdd.Add(k);
            if (model.Count > 100) toAdd.Add(model.Keys.ElementAt(model.Count / 2) + 1);
            toAdd = toAdd.Distinct().OrderBy(x => x).ToList();

            lookup.InitializeCursorState();
            foreach (var k in toAdd)
            {
                Int64LookupKey key = k;
                lookup.TryGetNextValue(ref key, out _);
                long value = k ^ round;
                lookup.AddOrSetAfterGetNext(ref key, value);
                model[k] = value;
            }

            Assert.Equal(model.Count, lookup.NumberOfEntries);
            if (round % 25 == 0)
            {
                lookup.VerifyStructure();
                foreach (var kv in model)
                {
                    Assert.True(lookup.TryGetValue(kv.Key, out long v), $"round {round} key {kv.Key}");
                    Assert.Equal(kv.Value, v);
                }
                output.WriteLine($"round {round}: entries={model.Count} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} root={string.Join(",", RootChildFlags(lookup))}");
            }
        }

        lookup.VerifyStructure();
        var it = lookup.Iterate();
        it.Reset();
        using var expected = model.GetEnumerator();
        while (it.MoveNext(out Int64LookupKey key, out long value, out _))
        {
            Assert.True(expected.MoveNext());
            Assert.Equal(expected.Current.Key, key.Value);
            Assert.Equal(expected.Current.Value, value);
        }
        Assert.False(expected.MoveNext());
    }

    // Finding probe: ShouldPromoteLeaf only inspects the sibling at index 0 (or 1). Build Root=[L1, L2, B3] by collapsing the
    // two leftmost branches, then split L2 and see whether the root gets a third leaf pointer (pre-fix behaviour).
    [RavenFact(RavenTestCategory.Voron)]
    public void TwoLeftmostLeaves_SplitOfSecondLeaf_IsNotWrapped()
    {
        long next = Stride;
        using var tx = Env.WriteTransaction();
        var lookup = tx.LookupFor<Int64LookupKey>("entries");

        while (lookup.State.BranchPages < 4) { lookup.Add(next, next); next += Stride; }
        // a middle branch that shrinks merges into its right sibling unless combined free space stays <= one page,
        // so the right-most branch has to be nearly full for the collapse to happen instead
        while (((LookupPageHeader*)lookup.Llt.GetPage(lookup.AllEntriesIn(lookup.State.RootPage)[^1].Item2).Pointer)->FreeSpace > 100) { lookup.Add(next, next); next += Stride; }
        Assert.Equal(4, lookup.State.BranchPages);
        output.WriteLine($"right-most branch free space = {((LookupPageHeader*)lookup.Llt.GetPage(lookup.AllEntriesIn(lookup.State.RootPage)[^1].Item2).Pointer)->FreeSpace}");
        Assert.Equal(new[] { LookupPageFlags.Branch, LookupPageFlags.Branch, LookupPageFlags.Branch }, RootChildFlags(lookup));
        output.WriteLine($"seeded entries={lookup.NumberOfEntries} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages}");

        CollapseBranchAt(lookup, 0);
        Assert.Equal(new[] { LookupPageFlags.Leaf, LookupPageFlags.Branch, LookupPageFlags.Branch }, RootChildFlags(lookup));

        CollapseBranchAt(lookup, 1);
        Assert.Equal(new[] { LookupPageFlags.Leaf, LookupPageFlags.Leaf, LookupPageFlags.Branch }, RootChildFlags(lookup));
        output.WriteLine($"after collapses: entries={lookup.NumberOfEntries} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} root={string.Join(",", RootChildFlags(lookup))}");

        // fill L2 until it splits
        var rootEntries = lookup.AllEntriesIn(lookup.State.RootPage);
        long l2Page = rootEntries[1].Item2;
        long l2First = rootEntries[1].Item1.Value;
        long l2Limit = rootEntries[2].Item1.Value;
        long leaves = lookup.State.LeafPages;
        long k = l2First + 1;
        while (lookup.State.LeafPages == leaves)
        {
            Assert.True(k < l2Limit);
            lookup.Add(k, k);
            k += 1 << 20;
        }
        var flags = RootChildFlags(lookup);
        output.WriteLine($"after L2 split: root={string.Join(",", flags)} branches={lookup.State.BranchPages} leaves={lookup.State.LeafPages} l2 is now {((LookupPageHeader*)lookup.Llt.GetPage(l2Page).Pointer)->PageFlags}");
        lookup.VerifyStructure();

        // documents current behaviour: the fix does not fire here
        Assert.Equal(new[] { LookupPageFlags.Leaf, LookupPageFlags.Leaf, LookupPageFlags.Leaf, LookupPageFlags.Branch }, flags);

        void CollapseBranchAt(Lookup<Int64LookupKey> l, int rootIndex)
        {
            // remove whole leaves of the branch at rootIndex, from its right end, until it has 2 leaves; then empty one more
            long branch = l.AllEntriesIn(l.State.RootPage)[rootIndex].Item2;
            while (true)
            {
                var children = l.AllEntriesIn(branch);
                Assert.True(children.Count >= 2);
                var victim = l.AllEntriesIn(children[^1].Item2);
                foreach (var (key, _) in victim)
                    Assert.True(l.TryRemove(key.Value, out _));
                if (children.Count == 2)
                    break;
            }
            Assert.True(((LookupPageHeader*)l.Llt.GetPage(branch).Pointer)->IsLeaf);
        }
    }

    private static LookupPageFlags[] RootChildFlags(Lookup<Int64LookupKey> lookup) =>
        lookup.AllEntriesIn(lookup.State.RootPage).Select(x => ((LookupPageHeader*)lookup.Llt.GetPage(x.Item2).Pointer)->PageFlags).ToArray();
}
