using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Voron;
using Voron.Data.BTrees;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// h-tree-model: randomized add / update / delete on regular Trees, value sizes spanning the
    /// inline / overflow / compressed-page boundaries, committing every transaction, child killed at
    /// a seeded random moment. The op stream is a pure function of (seed, tx index), so the parent
    /// replays the committed prefix (count read from a meta tree) into a model and compares the full
    /// key/value enumeration. Biased toward split/rebalance storms - the paths where the 16-byte
    /// TreePage struct is copied and search state must be published back.
    /// Two trees: one plain, one with LeafsCompressed.
    /// </summary>
    public static class HTreeModel
    {
        private const string PlainTree = "plain";
        private const string CompressedTree = "compressed";
        private const int KeySpace = 4000;

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

                ctx.RunChildAndKill(iteration, rng, minLifetimeMs: 2000, maxLifetimeMs: 9000);
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

            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree(PlainTree);
                tx.CreateTree(CompressedTree, flags: TreeFlags.LeafsCompressed);
                tx.Commit();
            }

            Console.WriteLine($"child up, pid {Environment.ProcessId}, dir {dir}");

            var plain = new Dictionary<long, string>();
            var compressed = new Dictionary<long, string>();

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            long t = 0;
            while (deadline.Elapsed.TotalSeconds < 30)
            {
                t++;
                using (var tx = env.WriteTransaction())
                {
                    var pTree = tx.CreateTree(PlainTree);
                    var cTree = tx.CreateTree(CompressedTree, flags: TreeFlags.LeafsCompressed);

                    GenerateTx(ctx.Seed, t, plain, (key, val) => Apply(pTree, key, val));
                    GenerateTx(ctx.Seed ^ 0x5eed, t, compressed, (key, val) => Apply(cTree, key, val));

                    tx.CreateTree("meta").Add("t", BitConverter.GetBytes(t));
                    tx.Commit();
                }

                if (t % 20 == 0)
                    env.FlushLogToDataFile();
            }

            return 0;
        }

        private static void Verify(StressContext ctx, string dir)
        {
            try
            {
                using var env = new StorageEnvironment(CreateOptions(dir));

                long committed = 0;
                using (var tx = env.ReadTransaction())
                {
                    var read = tx.ReadTree("meta")?.Read("t");
                    if (read != null)
                        committed = read.Reader.Read<long>();
                }

                if (committed == 0)
                {
                    ctx.VacuousIterations++;
                    return;
                }

                var plain = new Dictionary<long, string>();
                var compressed = new Dictionary<long, string>();
                for (long t = 1; t <= committed; t++)
                {
                    GenerateTx(ctx.Seed, t, plain, (_, _) => { });
                    GenerateTx(ctx.Seed ^ 0x5eed, t, compressed, (_, _) => { });
                }

                using (var tx = env.ReadTransaction())
                {
                    if (CompareTree(ctx, tx.ReadTree(PlainTree), plain, PlainTree, committed) == false)
                        return;
                    if (CompareTree(ctx, tx.ReadTree(CompressedTree), compressed, CompressedTree, committed) == false)
                        return;
                }

                Console.WriteLine($"  iter {ctx.Iteration}: verified {committed} txs, plain {plain.Count} / compressed {compressed.Count} keys");
            }
            catch (Exception e)
            {
                ctx.Fail($"recovery threw: {e}");
            }
        }

        private static bool CompareTree(StressContext ctx, Tree tree, Dictionary<long, string> model, string name, long committed)
        {
            if (tree == null)
            {
                if (model.Count == 0)
                    return true;
                ctx.Fail($"{name}: tree missing but model has {model.Count} keys after {committed} txs");
                return false;
            }

            var actual = new Dictionary<long, string>(model.Count);
            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var key = long.Parse(it.CurrentKey.ToString().Split('/')[1]);
                        actual[key] = it.CreateReaderForCurrent().ToStringValue();
                    } while (it.MoveNext());
                }
            }

            if (actual.Count != model.Count)
            {
                var missing = model.Keys.Except(actual.Keys).Take(5);
                var extra = actual.Keys.Except(model.Keys).Take(5);
                ctx.Fail($"{name}: count {actual.Count} != model {model.Count} after {committed} txs (missing {string.Join(",", missing)}; extra {string.Join(",", extra)})");
                return false;
            }

            foreach (var (key, expected) in model)
            {
                if (actual.TryGetValue(key, out var got) == false)
                {
                    ctx.Fail($"{name}: key {key} missing after {committed} txs");
                    return false;
                }

                if (got != expected)
                {
                    ctx.Fail($"{name}: key {key} value mismatch after {committed} txs (len {got.Length} vs {expected.Length})");
                    return false;
                }
            }

            return true;
        }

        private static void GenerateTx(int seed, long t, Dictionary<long, string> model, Action<long, string> apply)
        {
            var rng = new Random(unchecked(seed * 486187739 + (int)t * 1000003));

            // every ~40 txs, a split/rebalance storm: a burst of inserts, then a burst of deletes
            var storm = t % 40 < 4;
            var ops = storm ? 60 + rng.Next(80) : 5 + rng.Next(30);

            for (var i = 0; i < ops; i++)
            {
                var key = rng.Next(KeySpace);
                var roll = rng.Next(100);

                if (storm && t % 40 >= 2)
                {
                    // deletion-heavy tail of a storm
                    if (model.Remove(key) && apply != null)
                        apply(key, null);
                    continue;
                }

                if (roll < 70 || model.Count == 0)
                {
                    var val = ValueOf(key, rng);
                    model[key] = val;
                    apply?.Invoke(key, val);
                }
                else
                {
                    if (model.Remove(key))
                        apply?.Invoke(key, null);
                }
            }
        }

        private static void Apply(Tree tree, long key, string val)
        {
            if (val == null)
                tree.Delete(KeyOf(key));
            else
                tree.Add(KeyOf(key), val);
        }

        private static string ValueOf(long key, Random rng)
        {
            // span inline (<~700B), overflow (>8KB), and mid-size to force compression variety
            var size = rng.Next(4) switch
            {
                0 => 8 + rng.Next(120),
                1 => 400 + rng.Next(600),
                2 => 2000 + rng.Next(2000),
                _ => 9000 + rng.Next(8000)
            };
            var sb = new StringBuilder(size);
            var stamp = $"{key}:";
            while (sb.Length < size)
                sb.Append(stamp);
            return sb.ToString(0, size);
        }

        private static string KeyOf(long key) => $"k/{key:D7}";

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.MaxLogFileSize = 256 * 1024;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            return options;
        }
    }
}
