using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using Voron;
using Voron.Global;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// d-avalanche: a MEASUREMENT, not a pass/fail scenario. The PR defers the data-file sync until the unsynced backlog
    /// passes MaxUnsyncedBytesBeforeSync (256 MB) and, while trickle writeback is enabled, until it passes
    /// MaxUnsyncedBytesBeforeMandatorySync (768 MB) - WriteFlowPolicy.ShouldSyncNow. A later, bigger sync is cheaper in
    /// total but it is one large burst of I/O, and the question for the review (findings d-2 / s-12) is what that burst
    /// does to the commits that happen while it runs.
    ///
    /// One writer commits 0.5-1.5 MB transactions into a bounded key space with the real flusher and syncer. Every commit
    /// is timed; every data-file sync is taken from IoMetrics (DataSync meter: start, end, size). Per variant it reports
    /// commit latency inside and outside sync windows, the syncs themselves and the backlog peak.
    ///   defaults          - as shipped
    ///   no-deferral       - SyncWritebackBlockSizeInMb = 0, so ShouldSyncNow syncs as soon as the backlog passes 256 MB
    ///   small-thresholds  - 4 MB / 16 MB, i.e. small frequent syncs
    /// Windows note: trickle writeback is posix-only, so on Windows every sync is a full drain. The branch-environment
    /// variant (the d-2 claim) needs shared journals on Linux and is left to the Ubuntu track.
    /// </summary>
    public static class DAvalanche
    {
        private const int KeySpace = 1000;
        private const long BytesPerVariant = 4L * Constants.Size.Gigabyte;

        private static readonly (string Name, Action<StorageEnvironmentOptions> Configure)[] Variants =
        [
            ("defaults", _ => { }),
            ("no-deferral", o => o.SyncWritebackBlockSizeInMb = 0),
            ("small-thresholds", o =>
            {
                o.MaxUnsyncedBytesBeforeSync = 4 * Constants.Size.Megabyte;
                o.MaxUnsyncedBytesBeforeMandatorySync = 16 * Constants.Size.Megabyte;
            }),
        ];

        private readonly record struct Commit(DateTime End, double Ms, long Unsynced);

        private readonly record struct Sync(DateTime Start, DateTime End, long Size);

        public static int Run(StressContext ctx)
        {
            ctx.Iterations = Variants.Length;
            var perVariantBudget = TimeSpan.FromMinutes(Math.Max(0.5, ctx.Minutes / Variants.Length));
            var results = new List<object>();

            foreach (var (name, configure) in Variants)
            {
                var dir = Path.Combine(ctx.WorkDir, name);
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                Directory.CreateDirectory(dir);

                var (commits, syncs, written, journalSize) = Measure(dir, configure, perVariantBudget, new Random(ctx.Seed));
                results.Add(Summarize(name, commits, syncs, written, journalSize));

                Directory.Delete(dir, recursive: true); // several GB per variant, and the disk is not infinite
            }

            File.WriteAllText(Path.Combine(ctx.WorkDir, $"results-d-avalanche-{ctx.Seed}-measurements.json"),
                JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));

            return ctx.Finish();
        }

        private static (List<Commit> Commits, List<Sync> Syncs, long Written, long JournalSize) Measure(
            string dir, Action<StorageEnvironmentOptions> configure, TimeSpan budget, Random rng)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            configure(options);

            var commits = new List<Commit>(64 * 1024);
            var syncs = new Dictionary<DateTime, Sync>();
            long written = 0, txs = 0;

            using (var env = new StorageEnvironment(options))
            {
                var sp = Stopwatch.StartNew();
                var nextPoll = TimeSpan.FromSeconds(2);
                // pre-allocated values of mixed sizes, so allocation / GC does not show up in the commit latencies
                var values = Enumerable.Range(0, 8).Select(_ =>
                {
                    var buffer = new byte[Constants.Size.Megabyte / 2 + rng.Next(Constants.Size.Megabyte)];
                    rng.NextBytes(buffer);
                    return buffer;
                }).ToArray();

                while (sp.Elapsed < budget && written < BytesPerVariant)
                {
                    var value = values[txs % values.Length];
                    var size = value.Length;
                    var t0 = Stopwatch.GetTimestamp();
                    using (var tx = env.WriteTransaction())
                    {
                        tx.CreateTree("items").Add($"k/{txs % KeySpace:D6}", value);
                        tx.Commit();
                    }

                    var ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
                    commits.Add(new Commit(DateTime.UtcNow, ms, env.Journal.Applicator.TotalWrittenButUnsyncedBytes));
                    written += size;
                    txs++;

                    // the IoMetrics ring holds the last 256 items per meter, so collect as we go
                    if (sp.Elapsed > nextPoll)
                    {
                        nextPoll += TimeSpan.FromSeconds(2);
                        CollectSyncs(env, syncs);
                    }
                }

                // let the last sync land in the meters
                for (var i = 0; i < 50 && env.Journal.Applicator.TotalWrittenButUnsyncedBytes > 0; i++)
                    System.Threading.Thread.Sleep(100);
                CollectSyncs(env, syncs);

                return (commits, syncs.Values.OrderBy(s => s.Start).ToList(), written, env.Options.MaxLogFileSize);
            }
        }

        private static void CollectSyncs(StorageEnvironment env, Dictionary<DateTime, Sync> syncs)
        {
            foreach (var file in env.Options.IoMetrics.Files)
            {
                foreach (var item in file.DataSync.GetCurrentItems())
                {
                    if (item.End != default)
                        syncs[item.Start] = new Sync(item.Start, item.End, item.Size);
                }
            }
        }

        private static object Summarize(string name, List<Commit> commits, List<Sync> syncs, long written, long journalSize)
        {
            bool InSync(Commit c) => syncs.Any(s => c.End >= s.Start && c.End - TimeSpan.FromMilliseconds(c.Ms) <= s.End);

            var inside = commits.Where(InSync).Select(c => c.Ms).OrderBy(x => x).ToArray();
            var outside = commits.Where(c => InSync(c) == false).Select(c => c.Ms).OrderBy(x => x).ToArray();
            var syncMs = syncs.Select(s => (s.End - s.Start).TotalMilliseconds).OrderBy(x => x).ToArray();
            var syncMb = syncs.Select(s => s.Size / (double)Constants.Size.Megabyte).OrderBy(x => x).ToArray();
            var peak = commits.Count == 0 ? 0 : commits.Max(c => c.Unsynced);

            Console.WriteLine($"=== {name}: {commits.Count} commits, {written / Constants.Size.Megabyte} MB written, journal size {journalSize / Constants.Size.Megabyte} MB ===");
            Console.WriteLine($"  syncs:               {syncs.Count}, size MB p50 {P(syncMb, 50):0} max {P(syncMb, 100):0}, duration ms p50 {P(syncMs, 50):0} max {P(syncMs, 100):0}");
            Console.WriteLine($"  backlog peak:        {peak / Constants.Size.Megabyte} MB unsynced");
            Console.WriteLine($"  commit ms, outside a sync ({outside.Length,6}): p50 {P(outside, 50):0.00}  p99 {P(outside, 99):0.00}  max {P(outside, 100):0.00}");
            Console.WriteLine($"  commit ms, during a sync  ({inside.Length,6}): p50 {P(inside, 50):0.00}  p99 {P(inside, 99):0.00}  max {P(inside, 100):0.00}");

            return new
            {
                Variant = name,
                Commits = commits.Count,
                WrittenMb = written / Constants.Size.Megabyte,
                JournalSizeMb = journalSize / Constants.Size.Megabyte,
                Syncs = syncs.Count,
                SyncSizeMbP50 = P(syncMb, 50),
                SyncSizeMbMax = P(syncMb, 100),
                SyncMsP50 = P(syncMs, 50),
                SyncMsMax = P(syncMs, 100),
                BacklogPeakMb = peak / Constants.Size.Megabyte,
                CommitMsOutside = new { Count = outside.Length, P50 = P(outside, 50), P99 = P(outside, 99), Max = P(outside, 100) },
                CommitMsDuringSync = new { Count = inside.Length, P50 = P(inside, 50), P99 = P(inside, 99), Max = P(inside, 100) },
            };
        }

        private static double P(double[] sorted, int percentile)
        {
            if (sorted.Length == 0)
                return 0;
            var index = (int)Math.Ceiling(percentile / 100.0 * sorted.Length) - 1;
            return sorted[Math.Clamp(index, 0, sorted.Length - 1)];
        }
    }
}
