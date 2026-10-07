using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Fixed;
using Voron.Global;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// e-boundary-sweep: the deterministic counterpart to e-crash-model. Fuzzing finds the boundary
    /// cases of the fixed-size tree's tombstone layout eventually; this walks them on purpose.
    ///
    /// The interesting number is the per-page capacity, which the tombstone bitmap makes a function of
    /// the value size: FixedSizeTreePage.GetTombstonesLayout gives (usableSpace * 8) / (entrySize * 8 + 1).
    /// For each value size the sweep visits entry counts exactly at capacity-2 .. capacity+2 and around
    /// two and three full pages, and for each of those runs the shapes that move entries across the
    /// boundary: drain everything, leave exactly one live entry among all-tombstones (first, middle and
    /// last), delete the max key and then append a larger one (the append fast-path with a tombstoned
    /// max), resurrect the max key in place, delete every other entry and re-add them backwards, delete
    /// everything and re-add it (full tombstone repurpose), and carve a run out of the middle before
    /// appending past the end.
    ///
    /// Every case is verified three times: inside the writing transaction, from a fresh read
    /// transaction after the commit, and after closing and reopening the environment.
    /// </summary>
    public static unsafe class EBoundarySweep
    {
        private static readonly byte[] ValueSizes = [1, 2, 7, 8, 15, 16, 40, 100, 255];

        public static int Run(StressContext ctx)
        {
            var root = ctx.IterationDir(0);
            if (Directory.Exists(root))
                IOExtensions.DeleteDirectory(root);
            Directory.CreateDirectory(root);

            using var names = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(names, "fst", out var fstName);

            var cases = 0;
            foreach (var valSize in ValueSizes)
            {
                if (ctx.TimeLeft == false)
                    break;

                var capacity = FixedSizeTreePage<long>.GetTombstonesLayout(Constants.Storage.PageSize, sizeof(long) + valSize).Capacity;

                var counts = new[] { 1, 2, 3 }
                    .Concat(Around(capacity))
                    .Concat(Around(capacity * 2))
                    .Concat(Around(capacity * 3))
                    .Where(n => n > 0)
                    .Distinct()
                    .OrderBy(n => n)
                    .ToArray();

                foreach (var count in counts)
                {
                    foreach (var order in Enum.GetValues<Order>())
                    foreach (var shape in Enum.GetValues<Shape>())
                    {
                        if (ctx.TimeLeft == false)
                            break;

                        cases++;
                        ctx.Iteration = cases;
                        ctx.Iterations++;

                        var label = $"valSize={valSize} capacity={capacity} count={count} order={order} shape={shape}";
                        // IOExtensions.DeleteDirectory drains the pager's pending background disposals
                        // first, otherwise a just-closed environment can still hold a journal handle
                        var dir = Path.Combine(root, "case");
                        if (Directory.Exists(dir))
                            IOExtensions.DeleteDirectory(dir);
                        Directory.CreateDirectory(dir);

                        try
                        {
                            RunCase(ctx, dir, label, fstName, valSize, count, order, shape);
                        }
                        catch (Exception e)
                        {
                            ctx.Fail($"case {label}: threw {e}");
                        }

                        if (ctx.Failures.Count > 0)
                            return ctx.Finish();
                    }
                }

                Console.WriteLine($"  valSize {valSize}: capacity {capacity}, {cases} cases so far");
            }

            if (ctx.Failures.Count == 0)
                Console.WriteLine($"  clean: {cases} boundary cases, each verified in-transaction, after commit and after reopen");

            return ctx.Finish();
        }

        private static IEnumerable<int> Around(int n) => [n - 2, n - 1, n, n + 1, n + 2];

        private static IEnumerable<int> Insertions(int count, Order order)
        {
            var keys = Enumerable.Range(0, count).ToArray();
            switch (order)
            {
                case Order.Ascending:
                    return keys;
                case Order.Descending:
                    return keys.Reverse();
                case Order.Shuffled:
                    // deterministic shuffle - the sweep has to be replayable from the label alone
                    var rng = new Random(count * 397 + 17);
                    return keys.OrderBy(_ => rng.Next());
                default:
                    throw new ArgumentOutOfRangeException(nameof(order), order, null);
            }
        }

        // the append fast-path only engages for ascending keys; the other two orders force the general
        // insert path, where a tombstoned slot may be repurposed in the middle of a page
        private enum Order
        {
            Ascending,
            Descending,
            Shuffled
        }

        private enum Shape
        {
            AppendOnly,
            DrainAll,
            KeepFirstOnly,
            KeepMiddleOnly,
            KeepLastOnly,
            DeleteMaxThenAppendBigger,
            ResurrectMax,
            DeleteEveryOtherThenReAddBackwards,
            DeleteAllThenReAddAll,
            CarveMiddleThenAppendPastEnd
        }

        private static void RunCase(StressContext ctx, string dir, string label, Slice fstName, byte valSize, int count, Order order, Shape shape)
        {
            var model = new SortedDictionary<long, long>();

            using (var env = new StorageEnvironment(Options(dir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var fst = tx.CreateTree("parent").FixedTreeFor(fstName, valSize);

                    foreach (var i in Insertions(count, order))
                        Put(fst, model, i, valSize, i);

                    Apply(fst, model, valSize, count, shape);

                    Verify(ctx, fst, model, label, "inside the write transaction");
                    tx.Commit();
                }

                if (ctx.Failures.Count > 0)
                    return;

                using (var tx = env.ReadTransaction())
                {
                    var fst = tx.ReadTree("parent").FixedTreeFor(fstName, valSize);
                    Verify(ctx, fst, model, label, "after the commit");
                }
            }

            if (ctx.Failures.Count > 0)
                return;

            using (var env = new StorageEnvironment(Options(dir)))
            using (var tx = env.ReadTransaction())
            {
                var fst = tx.ReadTree("parent").FixedTreeFor(fstName, valSize);
                Verify(ctx, fst, model, label, "after reopening the environment");
            }
        }

        private static void Apply(FixedSizeTree fst, SortedDictionary<long, long> model, byte valSize, int count, Shape shape)
        {
            switch (shape)
            {
                case Shape.AppendOnly:
                    break;

                case Shape.DrainAll:
                    for (var i = 0; i < count; i++)
                        Remove(fst, model, i);
                    break;

                case Shape.KeepFirstOnly:
                    for (var i = 1; i < count; i++)
                        Remove(fst, model, i);
                    break;

                case Shape.KeepMiddleOnly:
                    for (var i = 0; i < count; i++)
                    {
                        if (i != count / 2)
                            Remove(fst, model, i);
                    }
                    break;

                case Shape.KeepLastOnly:
                    for (var i = 0; i < count - 1; i++)
                        Remove(fst, model, i);
                    break;

                case Shape.DeleteMaxThenAppendBigger:
                    Remove(fst, model, count - 1);
                    Put(fst, model, count, valSize, 0x5150);
                    break;

                case Shape.ResurrectMax:
                    Remove(fst, model, count - 1);
                    Put(fst, model, count - 1, valSize, 0x6161);
                    break;

                case Shape.DeleteEveryOtherThenReAddBackwards:
                    for (var i = 0; i < count; i += 2)
                        Remove(fst, model, i);
                    for (var i = (count - 1) / 2 * 2; i >= 0; i -= 2)
                        Put(fst, model, i, valSize, 0x7171 + i);
                    break;

                case Shape.DeleteAllThenReAddAll:
                    for (var i = 0; i < count; i++)
                        Remove(fst, model, i);
                    for (var i = 0; i < count; i++)
                        Put(fst, model, i, valSize, 0x8181 + i);
                    break;

                case Shape.CarveMiddleThenAppendPastEnd:
                    var from = count / 4;
                    var to = Math.Max(from, count - count / 4);
                    for (var i = from; i < to; i++)
                        Remove(fst, model, i);
                    for (var i = count; i < count + 5; i++)
                        Put(fst, model, i, valSize, 0x9191 + i);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(shape), shape, null);
            }
        }

        private static void Put(FixedSizeTree fst, SortedDictionary<long, long> model, long key, byte valSize, long stamp)
        {
            using (fst.DirectAdd(key, out _, out var ptr))
            {
                for (var b = 0; b < valSize; b++)
                    ptr[b] = (byte)((stamp + b) & 0xFF);
            }

            model[key] = stamp;
        }

        private static void Remove(FixedSizeTree fst, SortedDictionary<long, long> model, long key)
        {
            fst.Delete(key);
            model.Remove(key);
        }

        private static void Verify(StressContext ctx, FixedSizeTree fst, SortedDictionary<long, long> model, string label, string when)
        {
            if (fst.NumberOfEntries != model.Count)
            {
                ctx.Fail($"case {label}: {when}, NumberOfEntries is {fst.NumberOfEntries}, model has {model.Count}");
                return;
            }

            var seen = new List<long>(model.Count);
            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue))
                {
                    do
                    {
                        seen.Add(it.CurrentKey);
                    } while (it.MoveNext());
                }
            }

            var expected = model.Keys.ToArray();
            if (seen.Count != expected.Length)
            {
                var missing = expected.Except(seen).Take(5);
                var extra = seen.Except(expected).Take(5);
                ctx.Fail($"case {label}: {when}, iterated {seen.Count} keys, model has {expected.Length} (missing {string.Join(",", missing)}; extra {string.Join(",", extra)})");
                return;
            }

            for (var i = 0; i < seen.Count; i++)
            {
                if (seen[i] != expected[i])
                {
                    ctx.Fail($"case {label}: {when}, iteration diverges at position {i}: got key {seen[i]}, expected {expected[i]}");
                    return;
                }
            }

            // backward iteration walks the tombstone bitmap from the other end
            var back = new List<long>(model.Count);
            using (var it = fst.Iterate())
            {
                if (it.SeekToLast())
                {
                    do
                    {
                        back.Add(it.CurrentKey);
                    } while (it.MovePrev());
                }
            }

            back.Reverse();
            if (back.Count != expected.Length || back.Where((k, i) => k != expected[i]).Any())
            {
                ctx.Fail($"case {label}: {when}, backward iteration gives {back.Count} keys against {expected.Length} forward");
            }
        }

        private static StorageEnvironmentOptions Options(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.EnableJournalPoolPrewarming = false; // nothing here should outlive the case's directory
            return options;
        }
    }
}
