using System;
using System.IO;
using System.Threading;
using Sparrow.Platform;
using Voron;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// c-reader-soak: the ScratchPagesTable reclamation race, run long and mean. One writer mutates
    /// a fixed page set (with allocate/free churn, forced rebuilds, aggressive idle cleanup and
    /// ROLLED-BACK transactions writing poisoned stamps), heavily oversubscribed readers resolve
    /// pages through the scratch snapshot bypassing the tx page cache. Oracles per read:
    ///   1. the resolved page header and stamp match the requested page (wrong-chain detection),
    ///   2. a poisoned stamp (written only by rolled-back txs) is never visible,
    ///   3. the stamp's iteration is >= the version published before the read tx opened (staleness),
    ///   4. re-reading the same page inside one read tx returns the identical iteration (snapshot
    ///      immutability).
    /// Runs three variants back-to-back: plain, encrypted, 32-bit pager.
    /// </summary>
    public static unsafe class CReaderSoak
    {
        private const int PageCount = 64;
        private const long PoisonBit = 1L << 62;

        private enum Variant
        {
            Plain,
            Encrypted,
            Pager32Bit
        }

        public static int Run(StressContext ctx)
        {
            var variants = (Variant[])Enum.GetValues(typeof(Variant));
            var perVariantMinutes = ctx.Minutes / variants.Length;

            foreach (var variant in variants)
            {
                if (ctx.TimeLeft == false)
                    break;

                ctx.Iteration = (int)variant;
                ctx.Iterations++;
                var dir = ctx.IterationDir((int)variant);
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true); // never open a stale env (e.g. a prior crashed run) - a new master key can't decrypt it
                Directory.CreateDirectory(dir);

                try
                {
                    RunVariant(ctx, dir, variant, TimeSpan.FromMinutes(perVariantMinutes));
                }
                catch (Exception e)
                {
                    ctx.Fail($"variant {variant} threw: {e}");
                }

                if (ctx.Failures.Count > 0)
                    break;

                Directory.Delete(dir, recursive: true);
            }

            return ctx.Finish();
        }

        private static void RunVariant(StressContext ctx, string dir, Variant variant, TimeSpan duration)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            switch (variant)
            {
                case Variant.Encrypted:
                    options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
                    break;
                case Variant.Pager32Bit:
                    options.ForceUsing32BitsPager = true;
                    break;
            }

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

            using (var stop = new CancellationTokenSource(duration))
            {
                var token = stop.Token;

                var writer = new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(ctx.Seed + (int)variant);
                        var tempPage = -1L;
                        for (var iteration = 1L; token.IsCancellationRequested == false; iteration++)
                        {
                            var rollback = rng.Next(5) == 0;
                            var touched = new int[1 + rng.Next(16)];

                            using (var tx = env.WriteTransaction())
                            {
                                var llt = tx.LowLevelTransaction;
                                for (var i = 0; i < touched.Length; i++)
                                {
                                    var slot = rng.Next(PageCount);
                                    touched[i] = slot;
                                    var page = llt.ModifyPage(pages[slot]);
                                    *(long*)page.DataPointer = pages[slot];
                                    // a rolled-back tx writes a poisoned stamp - it must never become visible
                                    *((long*)page.DataPointer + 1) = rollback ? iteration | PoisonBit : iteration;
                                }

                                if (rollback == false)
                                {
                                    if (tempPage != -1)
                                        llt.FreePage(tempPage);
                                    tempPage = llt.AllocatePage(1).PageNumber;

                                    if (iteration % 512 == 511)
                                        env.ScratchPagesTable.ForceRebuildForTests();

                                    if (iteration % 977 == 0)
                                        env.ScratchPagesTable.IdleCleanup();

                                    tx.Commit();

                                    foreach (var slot in touched)
                                        Volatile.Write(ref publishedIteration[slot], iteration);
                                }
                                // else: dispose without commit - RollbackCurrentTransaction path
                            }

                            if (iteration % 64 == 0)
                                env.FlushLogToDataFile();
                        }
                    }
                    catch (Exception e)
                    {
                        Fail(e);
                    }
                });

                var readers = new Thread[Environment.ProcessorCount * 4];
                for (var r = 0; r < readers.Length; r++)
                {
                    var seed = r;
                    var holdReads = 256 << (r % 6);
                    readers[r] = new Thread(() =>
                    {
                        try
                        {
                            var rng = new Random(ctx.Seed ^ (17 + holdReads + seed));
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
                                            throw new InvalidOperationException(
                                                $"[{variant}] asked for page {pageNumber} but the snapshot resolved header {page.PageNumber}, stamp {stamp}");

                                        if ((iteration & PoisonBit) != 0)
                                            throw new InvalidOperationException(
                                                $"[{variant}] page {pageNumber} shows iteration {iteration & ~PoisonBit} written by a ROLLED-BACK transaction");

                                        if (iteration < floor[slot])
                                            throw new InvalidOperationException(
                                                $"[{variant}] page {pageNumber} resolved iteration {iteration}, older than {floor[slot]} which was durable before this read tx opened");

                                        if (seen[slot] == -1)
                                            seen[slot] = iteration;
                                        else if (seen[slot] != iteration)
                                            throw new InvalidOperationException(
                                                $"[{variant}] page {pageNumber} changed under a read transaction: {seen[slot]} -> {iteration} (snapshot immutability violated)");
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

                writer.Start();
                foreach (var reader in readers)
                    reader.Start();

                writer.Join();
                foreach (var reader in readers)
                    reader.Join();
            }

            if (failure != null)
            {
                ctx.Fail($"variant {variant}: {failure}");
                return;
            }

            Console.WriteLine($"  variant {variant}: clean after {duration.TotalMinutes:0.0} min");
        }
    }
}
