using System;
using System.IO;
using System.Threading;
using Voron;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// c-flush-race: the flush-vs-commit-vs-read races of the ScratchPagesTable made deterministic-ish.
    /// A dedicated flusher thread runs FlushLogToDataFile in a loop while the JournalApplicator test
    /// hooks make it PAUSE (seeded random 0-20ms) inside each of its four windows: under the write-tx
    /// lock while updating journal state, under the flushing lock, right before writing to the data
    /// file, and before/after assigning the post-flush journal state. Meanwhile one writer commits
    /// stamped pages (with rolled-back "poison" txs, allocate/free churn, forced rebuilds) and many
    /// readers resolve pages through the scratch snapshot bypassing the tx cache. Same oracles as
    /// c-reader-soak: right page, poison never visible, iteration floor, snapshot immutability.
    /// Run in RELEASE like c-reader-soak (Debug trips the VerifyMatch dictionary race, finding s-3).
    /// </summary>
    public static unsafe class CFlushRace
    {
        private const int PageCount = 96;
        private const long PoisonBit = 1L << 62;

        public static int Run(StressContext ctx)
        {
            ctx.Iterations = 1;
            var dir = ctx.IterationDir(0);
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
            Directory.CreateDirectory(dir);

            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true; // we drive the flusher ourselves, through the hooks
            options.ManualSyncing = true;
            options.EnableJournalPoolPrewarming = false;

            using var env = new StorageEnvironment(options);

            var pages = new long[PageCount];
            var publishedIteration = new long[PageCount];
            using (var tx = env.WriteTransaction())
            {
                var llt = tx.LowLevelTransaction;
                for (var i = 0; i < PageCount; i++)
                {
                    var page = llt.AllocatePage(1);
                    pages[i] = page.PageNumber;
                    *(long*)page.DataPointer = page.PageNumber;
                    *((long*)page.DataPointer + 1) = 0;
                }

                tx.Commit();
            }

            Exception failure = null;
            void Fail(Exception e) => Interlocked.CompareExchange(ref failure, e, null);

            // the flusher pauses inside each window; the seed makes the pause pattern replayable
            var hookRng = new Random(ctx.Seed);
            var hookLock = new object();
            void Pause()
            {
                int ms;
                lock (hookLock)
                    ms = hookRng.Next(21);
                if (ms > 0)
                    Thread.Sleep(ms);
            }

            var hooks = env.Journal.Applicator.ForTestingPurposesOnly();
            hooks.OnUpdateJournalStateUnderWriteTransactionLock = Pause;
            hooks.OnApplyLogsToDataFileUnderFlushingLock = Pause;
            hooks.OnApplyLogsToDataFile_BeforeWritingToDataFile = Pause;
            hooks.OnWaitForJournalStateToBeUpdated_BeforeAssigning_updateJournalStateAfterFlush = Pause;
            hooks.OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = Pause;

            var flushes = 0L;
            using (var stop = new CancellationTokenSource(TimeSpan.FromMinutes(ctx.Minutes)))
            {
                var token = stop.Token;

                var flusher = new Thread(() =>
                {
                    try
                    {
                        while (token.IsCancellationRequested == false)
                        {
                            env.FlushLogToDataFile();
                            Interlocked.Increment(ref flushes);
                            Thread.Sleep(1);
                        }
                    }
                    catch (Exception e)
                    {
                        Fail(e);
                    }
                });

                var writer = new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(ctx.Seed + 1);
                        var tempPage = -1L;
                        for (var iteration = 1L; token.IsCancellationRequested == false; iteration++)
                        {
                            var rollback = rng.Next(5) == 0;
                            var touched = new int[1 + rng.Next(12)];
                            using (var tx = env.WriteTransaction())
                            {
                                var llt = tx.LowLevelTransaction;
                                for (var i = 0; i < touched.Length; i++)
                                {
                                    var slot = rng.Next(PageCount);
                                    touched[i] = slot;
                                    var page = llt.ModifyPage(pages[slot]);
                                    *(long*)page.DataPointer = pages[slot];
                                    *((long*)page.DataPointer + 1) = rollback ? iteration | PoisonBit : iteration;
                                }

                                if (rollback == false)
                                {
                                    if (tempPage != -1)
                                        llt.FreePage(tempPage);
                                    tempPage = llt.AllocatePage(1).PageNumber;

                                    if (iteration % 256 == 255)
                                        env.ScratchPagesTable.ForceRebuildForTests();

                                    tx.Commit();

                                    foreach (var slot in touched)
                                        Volatile.Write(ref publishedIteration[slot], iteration);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Fail(e);
                    }
                });

                var readers = new Thread[Environment.ProcessorCount * 2];
                for (var r = 0; r < readers.Length; r++)
                {
                    var seed = r;
                    var holdReads = 128 << (r % 5);
                    readers[r] = new Thread(() =>
                    {
                        try
                        {
                            var rng = new Random(ctx.Seed ^ (101 + holdReads + seed));
                            var floor = new long[PageCount];
                            var seen = new long[PageCount];
                            while (token.IsCancellationRequested == false)
                            {
                                for (var i = 0; i < PageCount; i++)
                                {
                                    floor[i] = Volatile.Read(ref publishedIteration[i]);
                                    seen[i] = -1;
                                }

                                using (var tx = env.ReadTransaction())
                                {
                                    for (var i = 0; i < holdReads; i++)
                                    {
                                        var slot = rng.Next(PageCount);
                                        var pageNumber = pages[slot];
                                        var page = tx.LowLevelTransaction.GetPageWithoutCache(pageNumber);
                                        var stamp = *(long*)page.DataPointer;
                                        var iteration = *((long*)page.DataPointer + 1);

                                        if (page.PageNumber != pageNumber || stamp != pageNumber)
                                            throw new InvalidOperationException($"asked for page {pageNumber}, resolved header {page.PageNumber}, stamp {stamp}");
                                        if ((iteration & PoisonBit) != 0)
                                            throw new InvalidOperationException($"page {pageNumber} shows a stamp written by a ROLLED-BACK transaction (iteration {iteration & ~PoisonBit})");
                                        if (iteration < floor[slot])
                                            throw new InvalidOperationException($"page {pageNumber} resolved iteration {iteration}, older than {floor[slot]} published before this read tx opened");
                                        if (seen[slot] == -1)
                                            seen[slot] = iteration;
                                        else if (seen[slot] != iteration)
                                            throw new InvalidOperationException($"page {pageNumber} changed under a read transaction: {seen[slot]} -> {iteration}");
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Fail(e);
                        }
                    });
                }

                flusher.Start();
                writer.Start();
                foreach (var reader in readers)
                    reader.Start();

                writer.Join();
                flusher.Join();
                foreach (var reader in readers)
                    reader.Join();
            }

            if (failure != null)
                ctx.Fail(failure.ToString());
            else
                Console.WriteLine($"  clean: {Interlocked.Read(ref flushes)} paused flushes raced against commits/rollbacks/rebuilds and {Environment.ProcessorCount * 2} readers for {ctx.Minutes} min");

            return ctx.Finish();
        }
    }
}
