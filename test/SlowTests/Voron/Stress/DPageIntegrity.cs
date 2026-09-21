using System;
using System.Collections.Generic;
using System.IO;
using Voron;
using Voron.Global;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// d-page-integrity: end-to-end "no page is silently lost or mis-written by the paced writeback".
    /// The child runs with the REAL flusher and sync workers, frequent syncs, drain mode forced on
    /// (sync-cost threshold 0), small writeback blocks and a min-contiguous filter alternating per
    /// iteration. It stamps whole pages with (pageNumber, iteration, byte pattern) from a schedule that
    /// is a pure function of (seed, iteration) and records the iteration in a meta tree. The parent
    /// kills it, recovers, replays the schedule to the recovered iteration and checks every page
    /// byte-for-byte - first straight after recovery (journal replay over the written-back data file),
    /// then again after a forced flush+sync+reopen (everything served from the data file).
    /// Windows exercises the drain path; posix additionally exercises the trickle path.
    /// </summary>
    public static unsafe class DPageIntegrity
    {
        private const int PageCount = 512;
        private const int PatternBytes = 4096;

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
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                Directory.CreateDirectory(dir);

                ctx.RunChildAndKill(iteration, rng, minLifetimeMs: 3000, maxLifetimeMs: 12000);
                Verify(ctx, dir, iteration);

                if (ctx.Failures.Count > 0)
                    break;

                TryDelete(dir);
            }

            return ctx.Finish();
        }

        private static int RunChild(StressContext ctx)
        {
            var dir = ctx.IterationDir(ctx.Iteration);
            using var env = new StorageEnvironment(CreateOptions(dir, ctx.Iteration, manual: false));

            var pages = new long[PageCount];
            using (var tx = env.WriteTransaction())
            {
                var llt = tx.LowLevelTransaction;
                var index = tx.CreateTree("pages");
                for (var i = 0; i < PageCount; i++)
                {
                    var page = llt.AllocatePage(1);
                    pages[i] = page.PageNumber;
                    Stamp(page.DataPointer, page.PageNumber, 0, i);
                    index.Add(i.ToString("D5"), BitConverter.GetBytes(page.PageNumber));
                }

                tx.CreateTree("meta").Add("i", BitConverter.GetBytes(0L));
                tx.Commit();
            }

            Console.WriteLine($"child up, pid {Environment.ProcessId}, dir {dir}");

            var deadline = System.Diagnostics.Stopwatch.StartNew();
            long iteration = 0;
            while (deadline.Elapsed.TotalSeconds < 40)
            {
                iteration++;
                var slots = Schedule(ctx.Seed, iteration);
                using (var tx = env.WriteTransaction())
                {
                    var llt = tx.LowLevelTransaction;
                    foreach (var slot in slots)
                    {
                        var page = llt.ModifyPage(pages[slot]);
                        Stamp(page.DataPointer, pages[slot], iteration, slot);
                    }

                    tx.CreateTree("meta").Add("i", BitConverter.GetBytes(iteration));
                    tx.Commit();
                }

                if (iteration % 200 == 0)
                    System.Threading.Thread.Sleep(30); // let the flusher and the sync workers interleave
            }

            return 0;
        }

        private static void Verify(StressContext ctx, string dir, int iteration)
        {
            try
            {
                long recovered;
                using (var env = new StorageEnvironment(CreateOptions(dir, iteration, manual: true)))
                {
                    recovered = ReadIteration(env);
                    if (recovered == 0)
                    {
                        ctx.VacuousIterations++;
                        return;
                    }

                    var lastWriter = Replay(ctx.Seed, recovered);
                    if (CheckAllPages(ctx, env, lastWriter, recovered, "after recovery") == false)
                        return;

                    // push everything through the data file and read it back cold
                    env.FlushLogToDataFile();
                    env.SyncDataFileImmediately();
                }

                using (var env = new StorageEnvironment(CreateOptions(dir, iteration, manual: true)))
                {
                    var lastWriter = Replay(ctx.Seed, recovered);
                    if (CheckAllPages(ctx, env, lastWriter, recovered, "after flush+sync+reopen") == false)
                        return;
                }

                var minContiguous = iteration % 2 == 0 ? 4 : 64;
                Console.WriteLine($"  iter {iteration}: {recovered} committed iterations, {PageCount} pages verified twice (minContiguous {minContiguous}KB)");
            }
            catch (Exception e)
            {
                ctx.Fail($"recovery threw: {e}");
            }
        }

        private static bool CheckAllPages(StressContext ctx, StorageEnvironment env, long[] lastWriter, long recovered, string stage)
        {
            using var tx = env.ReadTransaction();
            var index = tx.ReadTree("pages");
            var llt = tx.LowLevelTransaction;
            for (var slot = 0; slot < PageCount; slot++)
            {
                var pageNumber = index.Read(slot.ToString("D5")).Reader.Read<long>();
                var page = llt.GetPage(pageNumber);
                var p = (long*)page.DataPointer;
                if (page.PageNumber != pageNumber || p[0] != pageNumber)
                {
                    ctx.Fail($"{stage}: slot {slot} page {pageNumber} resolved header {page.PageNumber}, stamp {p[0]} (recovered iteration {recovered})");
                    return false;
                }

                if (p[1] != lastWriter[slot])
                {
                    ctx.Fail($"{stage}: slot {slot} page {pageNumber} holds iteration {p[1]}, expected {lastWriter[slot]} (recovered iteration {recovered}) - a write-back lost or mis-ordered this page");
                    return false;
                }

                var expected = PatternByte(pageNumber, lastWriter[slot], slot);
                var bytes = (byte*)page.DataPointer + 16;
                for (var b = 0; b < PatternBytes; b++)
                {
                    if (bytes[b] != expected)
                    {
                        ctx.Fail($"{stage}: slot {slot} page {pageNumber} byte {b} is {bytes[b]}, expected {expected} - torn page content (iteration {lastWriter[slot]})");
                        return false;
                    }
                }
            }

            return true;
        }

        private static long ReadIteration(StorageEnvironment env)
        {
            using var tx = env.ReadTransaction();
            var read = tx.ReadTree("meta")?.Read("i");
            return read?.Reader.Read<long>() ?? 0;
        }

        private static long[] Replay(int seed, long upTo)
        {
            var lastWriter = new long[PageCount];
            for (long i = 1; i <= upTo; i++)
            {
                foreach (var slot in Schedule(seed, i))
                    lastWriter[slot] = i;
            }

            return lastWriter;
        }

        private static List<int> Schedule(int seed, long iteration)
        {
            var rng = new Random(unchecked(seed * 486187739 + (int)iteration * 1000003));
            var count = 1 + rng.Next(40);
            var slots = new List<int>(count);
            for (var i = 0; i < count; i++)
                slots.Add(rng.Next(PageCount));
            return slots;
        }

        private static void Stamp(byte* data, long pageNumber, long iteration, int slot)
        {
            var p = (long*)data;
            p[0] = pageNumber;
            p[1] = iteration;
            new Span<byte>(data + 16, PatternBytes).Fill(PatternByte(pageNumber, iteration, slot));
        }

        private static byte PatternByte(long pageNumber, long iteration, int slot) => (byte)(pageNumber * 31 + iteration * 17 + slot);

        // manual: the verifying parent drives flush/sync itself; the child runs the real flusher + sync workers
        private static StorageEnvironmentOptions CreateOptions(string dir, int iteration, bool manual)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            // syncing often, drain mode forced on so the paced writeback runs on Windows too
            options.ManualFlushing = manual;
            options.ManualSyncing = manual;
            options.MaxLogFileSize = Constants.Size.Megabyte;
            options.MaxNumberOfPagesInJournalBeforeFlush = 64;
            options.MaxUnsyncedBytesBeforeSync = Constants.Size.Megabyte;
            options.MaxUnsyncedBytesBeforeMandatorySync = 4 * Constants.Size.Megabyte;
            options.SyncWritebackBlockSizeInMb = 1;
            options.SyncWritebackMinContiguousSizeInKb = iteration % 2 == 0 ? 4 : 64;
            options.SyncWritebackBarrierCostThresholdInMs = 0;
            options.EnableJournalPoolPrewarming = false;
            return options;
        }

        private static void TryDelete(string dir)
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // a background thread may still hold a file - left for the OS temp cleanup
            }
        }
    }
}
