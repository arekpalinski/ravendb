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
    /// h-compressed-churn: compressed leaf pages hammered with the shapes that force decompression
    /// with Write usage, page merges (rebalance) and splits-on-decompressed, while the SAME write
    /// transaction reads back what it just wrote. That read-back is the new angle: it runs the
    /// decompression caches (Read entries vs Write entries, cache reuse at a grown page size,
    /// invalidation on ModifyPage / rebalance) against uncommitted uncompressed nodes and compression
    /// tombstones sitting on the original page.
    ///
    /// Per transaction: mutate, then verify in-tx (full enumeration + point reads for every key);
    /// after commit verify again from a read transaction. Several compressed trees share the env so
    /// freed page numbers get recycled across trees within a transaction.
    /// Run in RELEASE: a fully tombstoned compressed leaf stays in the tree as a zero-entry leaf
    /// (finding s-7, pre-existing), which trips a Debug.Assert that kills the process. In Release the
    /// same state surfaces as a catchable exception, which this scenario counts and steps over so the
    /// rest of the run keeps hunting.
    /// </summary>
    public static class HCompressedChurn
    {
        private const int TreeCount = 3;
        private const int KeySpace = 900;

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
            options.MaxLogFileSize = 4 * 1024 * 1024;

            var env = new StorageEnvironment(options);

            var names = Enumerable.Range(0, TreeCount).Select(i => $"c/{i}").ToArray();
            using (var tx = env.WriteTransaction())
            {
                foreach (var name in names)
                    tx.CreateTree(name, flags: TreeFlags.LeafsCompressed);
                tx.Commit();
            }

            var model = new Dictionary<long, string>[TreeCount];
            for (var i = 0; i < TreeCount; i++)
                model[i] = new Dictionary<long, string>();

            var rng = new Random(ctx.Seed);
            var sp = Stopwatch.StartNew();
            var budget = TimeSpan.FromMinutes(ctx.Minutes);
            long txs = 0, rollbacks = 0, ops = 0;
            var lastReport = -1;

            long emptyLeafHits = 0;
            while (sp.Elapsed < budget && ctx.Failures.Count == 0)
            {
                txs++;
                // 1 in 9 transactions is rolled back: the decompression caches and the tree header
                // mutations made inside it must leave nothing behind for the next transaction
                var rollback = rng.Next(9) == 0;
                var shadow = rollback
                    ? model.Select(m => new Dictionary<long, string>(m)).ToArray()
                    : model;

                using (var tx = env.WriteTransaction())
                {
                    var trees = names.Select(n => tx.CreateTree(n, flags: TreeFlags.LeafsCompressed)).ToArray();
                    ops += Mutate(rng, txs, trees, shadow);

                    if (Verify(ctx, trees, shadow, txs, rollback, ref emptyLeafHits) == false)
                        break;

                    if (rollback == false)
                        tx.Commit();
                    else
                        rollbacks++;
                }

                using (var tx = env.ReadTransaction())
                {
                    var trees = names.Select(n => tx.ReadTree(n)).ToArray();
                    if (Verify(ctx, trees, model, txs, committed: true, ref emptyLeafHits) == false)
                        break;
                }

                if (txs % 50 == 0)
                    env.FlushLogToDataFile();

                var tick = (int)(sp.Elapsed.TotalSeconds / 30);
                if (tick != lastReport)
                {
                    lastReport = tick;
                    Console.WriteLine($"  t+{sp.Elapsed.TotalSeconds:0}s: {txs} txs ({rollbacks} rolled back), {ops} ops, keys {string.Join("/", model.Select(m => m.Count))}");
                }
            }

            // final proof: reopen and compare, so nothing above was only true in memory
            env.Dispose();
            if (ctx.Failures.Count == 0)
            {
                using var reopened = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(dir));
                using var tx = reopened.ReadTransaction();
                var trees = names.Select(n => tx.ReadTree(n)).ToArray();
                Verify(ctx, trees, model, txs, committed: true, ref emptyLeafHits);
            }

            if (ctx.Failures.Count == 0)
                Console.WriteLine($"  clean: {txs} txs ({rollbacks} rolled back), {ops} ops over {TreeCount} compressed trees, {emptyLeafHits} zero-entry-leaf hits stepped over");

            return ctx.Finish();
        }

        private static int Mutate(Random rng, long tx, Tree[] trees, Dictionary<long, string>[] model)
        {
            var ops = 0;
            for (var t = 0; t < trees.Length; t++)
            {
                var tree = trees[t];
                var m = model[t];

                // phases rotate per tree per transaction so a tree that is mid-storm in one tx keeps
                // churning in the next one: fill pages to capacity, then merge them away, then
                // resurrect the same keys so freed overflow/leaf pages are reissued
                var phase = (int)((tx + t) % 6);

                switch (phase)
                {
                    case 0:
                    case 1:
                        ops += Fill(rng, tree, m, 40 + rng.Next(60));
                        break;

                    case 2:
                        ops += DeleteRange(rng, tree, m, 0.55);
                        break;

                    case 3:
                        // delete then re-add the SAME keys inside one transaction: the delete leaves a
                        // compression tombstone (or decompresses with Write usage and frees the
                        // overflow), the re-add lands as an uncompressed node on the same page
                        ops += DeleteAndReAdd(rng, tree, m, 30 + rng.Next(40));
                        break;

                    case 4:
                        // update in place, biased to values that change size class
                        ops += UpdateExisting(rng, tree, m, 30 + rng.Next(50));
                        break;

                    default:
                        // drain almost everything: forces page merges up the tree and root collapse
                        ops += DeleteRange(rng, tree, m, 0.9);
                        ops += Fill(rng, tree, m, 20 + rng.Next(30));
                        break;
                }
            }

            return ops;
        }

        private static int Fill(Random rng, Tree tree, Dictionary<long, string> model, int count)
        {
            for (var i = 0; i < count; i++)
            {
                var key = rng.Next(KeySpace);
                var value = ValueOf(key, rng);
                tree.Add(KeyOf(key), value);
                model[key] = value;
            }

            return count;
        }

        private static int DeleteRange(Random rng, Tree tree, Dictionary<long, string> model, double fraction)
        {
            var victims = model.Keys.OrderBy(k => k).ToArray();
            var take = (int)(victims.Length * fraction);
            var start = victims.Length == 0 ? 0 : rng.Next(victims.Length);
            var ops = 0;

            for (var i = 0; i < take; i++)
            {
                var key = victims[(start + i) % victims.Length];
                if (model.Remove(key) == false)
                    continue;
                tree.Delete(KeyOf(key));
                ops++;
            }

            return ops;
        }

        private static int DeleteAndReAdd(Random rng, Tree tree, Dictionary<long, string> model, int count)
        {
            var keys = model.Keys.ToArray();
            if (keys.Length == 0)
                return Fill(rng, tree, model, count);

            var ops = 0;
            for (var i = 0; i < count; i++)
            {
                var key = keys[rng.Next(keys.Length)];
                tree.Delete(KeyOf(key));
                model.Remove(key);
                ops++;

                if (rng.Next(4) == 0)
                    continue; // leave a few dead, so tombstones outnumber resurrections

                var value = ValueOf(key, rng);
                tree.Add(KeyOf(key), value);
                model[key] = value;
                ops++;
            }

            return ops;
        }

        private static int UpdateExisting(Random rng, Tree tree, Dictionary<long, string> model, int count)
        {
            var keys = model.Keys.ToArray();
            if (keys.Length == 0)
                return Fill(rng, tree, model, count);

            for (var i = 0; i < count; i++)
            {
                var key = keys[rng.Next(keys.Length)];
                var value = ValueOf(key, rng);
                tree.Add(KeyOf(key), value);
                model[key] = value;
            }

            return count;
        }

        private static bool Verify(StressContext ctx, Tree[] trees, Dictionary<long, string>[] model, long tx, bool committed, ref long emptyLeafHits)
        {
            try
            {
                return VerifyInTransaction(ctx, trees, model, tx, committed);
            }
            catch (InvalidOperationException e) when (e.Message.Contains("exceeds number of entries (0)"))
            {
                emptyLeafHits++;
                return true;
            }
        }

        private static bool VerifyInTransaction(StressContext ctx, Tree[] trees, Dictionary<long, string>[] model, long tx, bool committed)
        {
            var where = committed ? "after commit" : "inside the write tx";

            for (var t = 0; t < trees.Length; t++)
            {
                var tree = trees[t];
                var m = model[t];

                var seen = new Dictionary<long, string>(m.Count);
                using (var it = tree.Iterate(prefetch: false))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            var key = long.Parse(it.CurrentKey.ToString().Substring(2));
                            if (seen.ContainsKey(key))
                            {
                                ctx.Fail($"tx {tx} tree {t} {where}: key {key} enumerated twice");
                                return false;
                            }

                            seen[key] = it.CreateReaderForCurrent().ToStringValue();
                        } while (it.MoveNext());
                    }
                }

                if (seen.Count != m.Count)
                {
                    var missing = m.Keys.Except(seen.Keys).Take(5);
                    var extra = seen.Keys.Except(m.Keys).Take(5);
                    ctx.Fail($"tx {tx} tree {t} {where}: enumeration has {seen.Count} keys, model has {m.Count} (missing {string.Join(",", missing)}; extra {string.Join(",", extra)})");
                    return false;
                }

                foreach (var (key, expected) in m)
                {
                    if (seen.TryGetValue(key, out var got) == false)
                    {
                        ctx.Fail($"tx {tx} tree {t} {where}: key {key} not enumerated");
                        return false;
                    }

                    if (got != expected)
                    {
                        ctx.Fail($"tx {tx} tree {t} {where}: key {key} enumerated value differs (len {got.Length} vs expected {expected.Length})");
                        return false;
                    }

                    // point read goes through ReadDecompressed / the decompression caches, the
                    // iterator does not - they must agree
                    using (Slice.From(tree.Llt.Allocator, KeyOf(key), out Slice ks))
                    using (var read = tree.ReadDecompressed(ks))
                    {
                        if (read == null)
                        {
                            ctx.Fail($"tx {tx} tree {t} {where}: key {key} enumerates but ReadDecompressed returns null");
                            return false;
                        }

                        if (read.Reader.ToStringValue() != expected)
                        {
                            ctx.Fail($"tx {tx} tree {t} {where}: key {key} ReadDecompressed value differs from the model (len {read.Reader.Length} vs expected {expected.Length})");
                            return false;
                        }
                    }
                }

                // absent keys must stay absent - a stale decompressed page would resurrect one
                for (var probe = 0; probe < 32; probe++)
                {
                    var key = (int)((tx * 7919 + probe * 104729) % KeySpace);
                    if (m.ContainsKey(key))
                        continue;

                    using var probeKey = Slice.From(tree.Llt.Allocator, KeyOf(key), out Slice absentKey);
                    using var absent = tree.ReadDecompressed(absentKey);
                    if (absent != null)
                    {
                        ctx.Fail($"tx {tx} tree {t} {where}: key {key} was deleted but ReadDecompressed still returns a value");
                        return false;
                    }
                }
            }

            return true;
        }

        private static string ValueOf(long key, Random rng)
        {
            // sized around the compressed-leaf boundaries: small nodes pack many per page, the ~1-4KB
            // band makes a page reach capacity after a handful of entries, and >8KB goes to overflow
            // (a PageRef node inside a compressed page - the shape whose lifetime is easiest to get wrong)
            var size = rng.Next(8) switch
            {
                0 or 1 or 2 => 16 + rng.Next(200),
                3 or 4 => 700 + rng.Next(1200),
                5 or 6 => 2500 + rng.Next(2500),
                _ => 8200 + rng.Next(24000)
            };

            var sb = new StringBuilder(size);
            var stamp = $"{key}#{size}|";
            while (sb.Length < size)
                sb.Append(stamp);
            return sb.ToString(0, size);
        }

        private static string KeyOf(long key) => $"k/{key:D6}";
    }
}
