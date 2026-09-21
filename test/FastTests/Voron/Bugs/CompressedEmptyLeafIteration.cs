using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using Raven.Server.Utils;
using Voron.Impl.Compaction;
using FastTests.Voron;
using Voron;
using Voron.Data.BTrees;
using Xunit;


namespace FastTests.Voron.Bugs
{
    public class CompressedEmptyLeafIteration : StorageTest
    {
        public CompressedEmptyLeafIteration(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IteratingOverALeafWhoseEntriesWereAllTombstonedMustNotReadPastTheEntries()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("c", flags: TreeFlags.LeafsCompressed);
                tx.Commit();
            }

            // values big enough that a compressed leaf holds only a handful of them, so the whole
            // page can be tombstoned without ever running out of space for the tombstone nodes
            // (which is what would otherwise force the decompress + rebalance path)
            var expected = new Dictionary<string, string>();
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("c", flags: TreeFlags.LeafsCompressed);
                for (var i = 0; i < 60; i++)
                {
                    var key = $"k/{i:D4}";
                    var value = Value(i);
                    tree.Add(key, value);
                    expected[key] = value;
                }

                tx.Commit();
            }

            // delete a contiguous run in the middle - enough to empty out whole leaves, not enough
            // to empty the tree
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("c", flags: TreeFlags.LeafsCompressed);
                for (var i = 10; i < 45; i++)
                {
                    var key = $"k/{i:D4}";
                    tree.Delete(key);
                    expected.Remove(key);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("c");
                var seen = new List<string>();
                using (var it = tree.Iterate(prefetch: false))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            seen.Add(it.CurrentKey.ToString());
                        } while (it.MoveNext());
                    }
                }

                Assert.Equal(expected.Keys.OrderBy(x => x).ToArray(), seen.ToArray());
            }
        }

        // the same tree state, seen through StorageCompaction - which iterates the source tree, and is
        // one of the two places in the product that even creates LeafsCompressed trees
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CompactingAnEnvironmentWithAFullyTombstonedCompressedLeafMustSucceed(bool deleteARun)
        {
            var source = Path.Combine(DataDir, "Source");
            var compactedData = Path.Combine(DataDir, "Compacted");
            IOExtensions.DeleteDirectory(source);
            IOExtensions.DeleteDirectory(compactedData);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(source)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("c", flags: TreeFlags.LeafsCompressed);
                    for (var i = 0; i < 60; i++)
                        tree.Add($"k/{i:D4}", Value(i));

                    tx.Commit();
                }

                if (deleteARun)
                {
                    using var tx = env.WriteTransaction();
                    var tree = tx.CreateTree("c", flags: TreeFlags.LeafsCompressed);
                    for (var i = 10; i < 45; i++)
                        tree.Delete($"k/{i:D4}");

                    tx.Commit();
                }
            }

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPathForTests(source),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPathForTests(compactedData));

            using var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(compactedData));
            using var read = compacted.ReadTransaction();

            var compactedTree = read.ReadTree("c");
            Assert.True(compactedTree != null, "the compacted environment has no tree 'c'");
            Assert.Equal(deleteARun ? 25 : 60, compactedTree.ReadHeader().NumberOfEntries);
        }

        private static string Value(int i)
        {
            var sb = new StringBuilder(3000);
            while (sb.Length < 3000)
                sb.Append($"{i}:");
            return sb.ToString(0, 3000);
        }
    }
}
