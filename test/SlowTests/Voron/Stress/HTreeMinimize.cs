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
    /// h-tree-minimize: delta-debugging for the compressed-leaf failure found by h-tree-model.
    /// Regenerates the deterministic op stream for a seed, confirms the failure in-process, then
    /// shrinks it (first whole transactions, then single ops) while the failure signature persists.
    /// Writes the minimal sequence as a ready-to-paste C# test body.
    /// </summary>
    public static class HTreeMinimize
    {
        private sealed record Op(long Key, int ValueSize); // ValueSize < 0 = delete

        public static int Run(StressContext ctx)
        {
            var scratch = Path.Combine(ctx.WorkDir, "min");
            Directory.CreateDirectory(ctx.WorkDir);

            var txs = GenerateCompressedTreeOps(ctx.Seed, maxTx: 400);
            var signature = Fails(scratch, txs, out var failingTx);
            static bool IsBug(string sig) => sig is "double-free" or "dangling-page-ref";
            if (signature == null)
            {
                ctx.Fail($"seed {ctx.Seed}: the compressed-tree op stream does not fail within 400 txs");
                return ctx.Finish();
            }

            txs = txs.Take(failingTx).ToList();
            Console.WriteLine($"seed {ctx.Seed}: fails at tx {failingTx} with '{signature}' - {txs.Sum(t => t.Count)} ops in {txs.Count} txs");

            // phase 1: drop whole transactions (ddmin, granularity halving)
            txs = DeltaMinimize(txs, candidate => IsBug(Fails(scratch, candidate, out _)), "txs", ctx);

            // phase 2: drop individual ops inside the surviving transactions
            var flat = txs.SelectMany((t, i) => t.Select(op => (Tx: i, Op: op))).ToList();
            var minimalFlat = DeltaMinimize(flat, candidate => IsBug(Fails(scratch, Regroup(candidate), out _)), "ops", ctx);
            txs = Regroup(minimalFlat);

            var final = Fails(scratch, txs, out _);
            Console.WriteLine($"minimal: {txs.Sum(t => t.Count)} ops in {txs.Count} txs, signature '{final}'");

            var path = Path.Combine(ctx.WorkDir, $"minimal-repro-seed-{ctx.Seed}.cs.txt");
            File.WriteAllText(path, RenderTest(txs, ctx.Seed, final));
            Console.WriteLine($"repro written to {path}");
            Console.WriteLine(RenderTest(txs, ctx.Seed, final));

            return ctx.Finish();
        }

        private static List<List<Op>> Regroup(List<(int Tx, Op Op)> flat) =>
            flat.GroupBy(x => x.Tx).OrderBy(g => g.Key).Select(g => g.Select(x => x.Op).ToList()).ToList();

        private static List<T> DeltaMinimize<T>(List<T> items, Func<List<T>, bool> stillFails, string what, StressContext ctx)
        {
            var n = 2;
            while (items.Count >= 2)
            {
                var chunk = (int)Math.Ceiling(items.Count / (double)n);
                var reduced = false;
                for (var start = 0; start < items.Count; start += chunk)
                {
                    if (ctx.TimeLeft == false)
                        return items;

                    var complement = items.Take(start).Concat(items.Skip(start + chunk)).ToList();
                    if (complement.Count == 0)
                        continue;

                    if (stillFails(complement))
                    {
                        items = complement;
                        n = Math.Max(n - 1, 2);
                        reduced = true;
                        Console.WriteLine($"  {what}: {items.Count} left");
                        break;
                    }
                }

                if (reduced)
                    continue;
                if (n >= items.Count)
                    break;
                n = Math.Min(items.Count, n * 2);
            }

            return items;
        }

        // returns a failure signature or null; failingTx = 1-based index of the tx that threw
        private static string Fails(string dir, List<List<Op>> txs, out int failingTx)
        {
            failingTx = 0;
            // a fresh directory per attempt: the previous env's recyclable journal can still be held by a
            // background thread (pager-state disposal queue / journal prewarming) when we return
            dir = Path.Combine(dir, Guid.NewGuid().ToString("N"));

            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.EnableJournalPoolPrewarming = false;

            try
            {
                using var env = new StorageEnvironment(options);
                for (var i = 0; i < txs.Count; i++)
                {
                    failingTx = i + 1;
                    using var tx = env.WriteTransaction();
                    var tree = tx.CreateTree("compressed", flags: TreeFlags.LeafsCompressed);
                    foreach (var op in txs[i])
                    {
                        if (op.ValueSize < 0)
                            tree.Delete(KeyOf(op.Key));
                        else
                            tree.Add(KeyOf(op.Key), ValueOf(op.Key, op.ValueSize));
                    }

                    tx.Commit();
                }
            }
            catch (Exception e) when (e.Message.Contains("already has the requested value"))
            {
                return "double-free";
            }
            catch (Exception e) when (e.Message.Contains("was not allocated"))
            {
                return "dangling-page-ref";
            }
            catch (Exception e)
            {
                return "other:" + e.GetType().Name;
            }

            failingTx = 0;
            return null;
        }

        // the exact generator of HTreeModel for the compressed tree, recording ops instead of applying them
        private static List<List<Op>> GenerateCompressedTreeOps(int seed, int maxTx)
        {
            var model = new Dictionary<long, string>();
            var txs = new List<List<Op>>();
            var treeSeed = seed ^ 0x5eed;
            for (long t = 1; t <= maxTx; t++)
            {
                var ops = new List<Op>();
                var rng = new Random(unchecked(treeSeed * 486187739 + (int)t * 1000003));
                var storm = t % 40 < 4;
                var count = storm ? 60 + rng.Next(80) : 5 + rng.Next(30);
                for (var i = 0; i < count; i++)
                {
                    var key = rng.Next(4000);
                    var roll = rng.Next(100);
                    if (storm && t % 40 >= 2)
                    {
                        if (model.Remove(key))
                            ops.Add(new Op(key, -1));
                        continue;
                    }

                    if (roll < 70 || model.Count == 0)
                    {
                        var size = SizeOf(rng);
                        model[key] = size.ToString();
                        ops.Add(new Op(key, size));
                    }
                    else if (model.Remove(key))
                    {
                        ops.Add(new Op(key, -1));
                    }
                }

                txs.Add(ops);
            }

            return txs;
        }

        private static int SizeOf(Random rng) => rng.Next(4) switch
        {
            0 => 8 + rng.Next(120),
            1 => 400 + rng.Next(600),
            2 => 2000 + rng.Next(2000),
            _ => 9000 + rng.Next(8000)
        };

        private static string ValueOf(long key, int size)
        {
            var sb = new StringBuilder(size);
            var stamp = $"{key}:";
            while (sb.Length < size)
                sb.Append(stamp);
            return sb.ToString(0, size);
        }

        private static string KeyOf(long key) => $"k/{key:D7}";

        private static string RenderTest(List<List<Op>> txs, int seed, string signature)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"// minimal repro (seed {seed}, signature: {signature}) - {txs.Sum(t => t.Count)} ops in {txs.Count} txs");
            sb.AppendLine("// tree: tx.CreateTree(\"compressed\", flags: TreeFlags.LeafsCompressed); value = key-stamped string of the given length");
            for (var i = 0; i < txs.Count; i++)
            {
                sb.AppendLine("using (var tx = Env.WriteTransaction())");
                sb.AppendLine("{");
                sb.AppendLine("    var tree = tx.CreateTree(\"compressed\", flags: TreeFlags.LeafsCompressed);");
                foreach (var op in txs[i])
                {
                    sb.AppendLine(op.ValueSize < 0
                        ? $"    tree.Delete(\"{KeyOf(op.Key)}\");"
                        : $"    tree.Add(\"{KeyOf(op.Key)}\", new string('x', {op.ValueSize}));");
                }
                sb.AppendLine("    tx.Commit();");
                sb.AppendLine("}");
            }
            return sb.ToString();
        }
    }
}
