using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // Table keeps one FixedSizeTree instance per Table object for every tree-index key (Table.GetFixedSizeTree cache),
    // so two tables that share a global index - every Collection.Revisions.* table shares DeleteRevisionEtag and
    // ResolvedFlagByEtag - hold two FixedSizeTree instances over the same on-disk tree inside one write transaction.
    // The append fast path remembers the rightmost leaf per instance; when the other instance splits that leaf, the
    // first instance appends its next key into a leaf that is no longer the rightmost and the key becomes unreachable
    // by search. This is what g-cache-assert hits as "Invalid index ... attempted to delete value but the value from
    // <id> wasn't in the index" out of RevertRevisionsOperation (finding s-13).
    public class FixedSizeTreeStaleAppendCache : StorageTest
    {
        public FixedSizeTreeStaleAppendCache(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AppendAfterAnotherInstanceSplitTheRightmostLeafMustStayReachable()
        {
            using (var tx = Env.WriteTransaction())
            {
                Tree parent = tx.CreateTree("parent");
                Slice.From(tx.Allocator, "shared", ByteStringType.Immutable, out Slice name);

                var first = new FixedSizeTree(tx.LowLevelTransaction, parent, name, 0);

                // sequential appends through the first instance: its append cache ends up pointing at the rightmost leaf
                const long step = 1000;
                const long max = 3_000 * step;
                for (long key = step; key <= max; key += step)
                    Assert.True(first.Add(key));

                // a second instance over the same tree, opened once the tree exists - Table.GetFixedSizeTree creates
                // one per Table object on first use, so two tables sharing a global index get here in one transaction
                var second = new FixedSizeTree(tx.LowLevelTransaction, parent, name, 0);

                // the second instance inserts just below the maximum, i.e. inside that rightmost leaf and not at its end,
                // until the leaf fills and splits 3/4 - 1/4: the original page keeps the lower part and has room again,
                // the top part (including the maximum) moves to a new page to the right
                long insertedBySecond = 0;
                for (long key = max - 1; key >= max - 1_500; key--)
                {
                    if (key % step == 0)
                        continue; // already there from the first loop

                    Assert.True(second.Add(key), $"{key} was reported as already present by the second instance (after {insertedBySecond} inserts through it)");
                    insertedBySecond++;
                }

                // the first instance still believes its cached page is the rightmost leaf and appends there
                Assert.True(first.Add(max + 1));

                var fresh = new FixedSizeTree(tx.LowLevelTransaction, parent, name, 0);

                Assert.Equal(3_000 + insertedBySecond + 1, fresh.NumberOfEntries);
                Assert.True(fresh.Contains(max + 1), "the key appended through the stale instance is not reachable by search");
                Assert.True(first.Contains(max + 1));
                Assert.True(second.Contains(max + 1));

                AssertStrictlyAscending(fresh);

                Assert.Equal(1, fresh.Delete(max + 1).NumberOfEntriesDeleted);
            }
        }

        private static void AssertStrictlyAscending(FixedSizeTree tree)
        {
            long visited = 0;
            long previous = long.MinValue;
            using (var it = tree.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    return;

                do
                {
                    long current = it.CurrentKey;
                    Assert.True(current > previous, $"entry {visited}: {current} follows {previous}, the tree is out of order");
                    previous = current;
                    visited++;
                } while (it.MoveNext());
            }

            Assert.Equal(tree.NumberOfEntries, visited);
        }
    }
}
