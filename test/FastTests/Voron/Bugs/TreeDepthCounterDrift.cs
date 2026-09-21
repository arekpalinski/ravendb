using System;
using System.Collections.Generic;
using System.Text;
using FastTests.Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public unsafe class TreeDepthCounterDrift : StorageTest
    {
        public TreeDepthCounterDrift(ITestOutputHelper output) : base(output)
        {
        }

        // keys near Constants.Tree.MaxKeySize so a branch page holds ~2 separators and the tree gets
        // many levels deep from a few hundred entries
        private const int KeySize = 3800;

        [Fact]
        public void HeaderDepthMustMatchTheActualNumberOfLevels()
        {
            var rng = new Random(1);
            var model = new SortedSet<int>();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("t");
                tx.Commit();
            }

            for (var round = 0; round < 120; round++)
            {
                using var tx = Env.WriteTransaction();
                var tree = tx.CreateTree("t");

                if (model.Count < 700 || rng.Next(3) != 0)
                {
                    for (var i = 0; i < 60; i++)
                    {
                        var key = rng.Next(6000);
                        tree.Add(KeyOf(key), "v");
                        model.Add(key);
                    }
                }
                else
                {
                    var keys = new List<int>(model);
                    var start = rng.Next(keys.Count);
                    var take = Math.Min(keys.Count - start, 30 + rng.Next(150));
                    for (var i = 0; i < take; i++)
                    {
                        tree.Delete(KeyOf(keys[start + i]));
                        model.Remove(keys[start + i]);
                    }
                }

                var reported = tree.ReadHeader().Depth;
                var actual = RealDepth(tree);
                Assert.True(reported == actual, $"round {round}: header reports Depth {reported}, the tree is {actual} levels deep ({model.Count} keys)");

                tx.Commit();
            }
        }

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

        private static string KeyOf(int key)
        {
            var sb = new StringBuilder(KeySize);
            sb.Append($"k/{key:D6}/");
            while (sb.Length < KeySize)
                sb.Append('p');
            return sb.ToString(0, KeySize);
        }
    }
}
