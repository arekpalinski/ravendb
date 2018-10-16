using System;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10382 : StorageTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                tx.Commit();
            }

            var random = new Random(1);
            var bytes = new byte[1024 * 8];

            // insert
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                for (int i = 0; i < 40_000; i++)
                {
                    random.NextBytes(bytes);

                    tree.Add(GetKey(i), new MemoryStream(bytes));
                }

                tx.Commit();
            }

            // delete
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                for (int i = 0; i < 40_000; i += 4)
                {
                    tree.Delete(GetKey(i));
                }

                tree.Delete(GetKey(33392));

                for (int i = 25_000; i >= 0 ; i--)
                {
                    tree.Delete(GetKey(i));

                    tree.ValidateTree_References();
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                using (var it = tree.Iterate(prefetch: false))
                {
                    Assert.True(it.Seek(Slices.BeforeAllKeys));

                    var count = 0;
                    do
                    {
                        var key = it.CurrentKey.ToString();
                        Assert.Equal(GetKey(25_000 + count), key);

                        count++;
                    } while (it.MoveNext());

                    Assert.Equal(15_000, count);
                }
            }
        }

        private static string GetKey(int i)
        {
            return $"{i:D19}.{i:D19}.{i:D19}.{i:D19}.{i:D19}";
        }
    }
}
