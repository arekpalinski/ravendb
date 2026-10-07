using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Voron;
using Voron.Impl.Scratch;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// c-snapshot-model: the lifetime of the ScratchPagesTable's array generations, as seen by a reader
    /// that outlives many of them. A ScratchPagesSnapshot is a struct holding the table's keys, heads
    /// and entries arrays directly; a Rebuild allocates new ones and retires the old ones, and
    /// ReclaimRetiredGenerations hands them back to the pool once the prune floor says nobody can see
    /// them any more. Get that floor wrong by one active transaction and a live reader's snapshot is
    /// looking at arrays that have been handed to a later generation and overwritten.
    ///
    /// The oracle needs no model of the table's semantics, only its central promise: a snapshot is
    /// immutable. Each reader opens a read transaction, enumerates its snapshot once into a shadow, and
    /// then re-enumerates it repeatedly while the writer commits and rebuilds underneath - every pass
    /// must produce exactly the same set of pages with exactly the same scratch positions and sizes,
    /// and TryGetValue must agree with the shadow, until the transaction is closed.
    ///
    /// The writer rebuilds on nearly every commit and flushes regularly, so generations retire far
    /// faster than in c-reader-soak, which is what makes the reclaim floor the thing under test.
    /// Run in RELEASE: Debug trips the unrelated VerifyMatch dictionary race (finding s-3).
    /// </summary>
    public static unsafe class CSnapshotModel
    {
        private const int PageCount = 256;

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
            options.EnableJournalPoolPrewarming = false;

            using var env = new StorageEnvironment(options);

            var pages = new long[PageCount];
            using (var tx = env.WriteTransaction())
            {
                var llt = tx.LowLevelTransaction;
                for (var i = 0; i < PageCount; i++)
                {
                    var page = llt.AllocatePage(1);
                    pages[i] = page.PageNumber;
                    *(long*)page.DataPointer = page.PageNumber;
                }

                tx.Commit();
            }

            Exception failure = null;
            void Fail(Exception e) => Interlocked.CompareExchange(ref failure, e, null);

            long rebuilds = 0, commits = 0, passes = 0, snapshots = 0;

            using (var stop = new CancellationTokenSource(TimeSpan.FromMinutes(ctx.Minutes)))
            {
                var token = stop.Token;

                var writer = new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(ctx.Seed);
                        var temps = new List<long>();
                        for (var iteration = 1L; token.IsCancellationRequested == false; iteration++)
                        {
                            var allocated = new List<long>();
                            var freed = 0;
                            using (var tx = env.WriteTransaction())
                            {
                                var llt = tx.LowLevelTransaction;

                                for (var i = 0; i < 1 + rng.Next(16); i++)
                                {
                                    var page = llt.ModifyPage(pages[rng.Next(PageCount)]);
                                    *((long*)page.DataPointer + 1) = iteration;
                                }

                                // allocate/free churn keeps entries being taken from and returned to the
                                // entry pool, which is what makes a generation worth retiring
                                for (var i = 0; i < rng.Next(6); i++)
                                    allocated.Add(llt.AllocatePage(1).PageNumber);

                                while (temps.Count - freed > 24)
                                    llt.FreePage(temps[freed++]);

                                // far more aggressive than anything the product does - the point is to
                                // retire generations faster than the readers can let go of them. Inside
                                // the write transaction, because Rebuild expects the write lock (same as
                                // IdleCleanup, and the same place c-flush-race calls it from)
                                env.ScratchPagesTable.ForceRebuildForTests();
                                Interlocked.Increment(ref rebuilds);

                                if (rng.Next(8) != 0) // most transactions commit, some roll back
                                {
                                    tx.Commit();
                                    Interlocked.Increment(ref commits);

                                    // only a committed transaction changes which pages exist; a rollback
                                    // un-allocates and un-frees everything above
                                    temps.RemoveRange(0, freed);
                                    temps.AddRange(allocated);
                                }
                            }

                            if (iteration % 64 == 0)
                                env.FlushLogToDataFile(); // RemoveFlushed, so entries actually leave the table
                        }
                    }
                    catch (Exception e)
                    {
                        Fail(e);
                    }
                });

                var readers = new Thread[Math.Max(4, Environment.ProcessorCount)];
                for (var r = 0; r < readers.Length; r++)
                {
                    // staggered: some readers hold a snapshot across a handful of rebuilds, some across
                    // thousands, so the reclaim floor is tested at every age
                    var rereads = 8 << (r % 6);
                    readers[r] = new Thread(() =>
                    {
                        try
                        {
                            while (token.IsCancellationRequested == false)
                            {
                                using var tx = env.ReadTransaction();
                                var snapshot = tx.LowLevelTransaction.CurrentStateRecord.ScratchPagesTable;
                                if (snapshot.IsValid == false)
                                    continue;

                                Interlocked.Increment(ref snapshots);
                                var shadow = Materialize(snapshot);

                                for (var i = 0; i < rereads && token.IsCancellationRequested == false; i++)
                                {
                                    var again = Materialize(snapshot);
                                    Compare(snapshot.VisibleAsOfSeq, shadow, again, i);

                                    foreach (var (pageNumber, expected) in shadow)
                                    {
                                        if (snapshot.TryGetValue(pageNumber, out var got) == false)
                                            throw new InvalidOperationException($"snapshot seq {snapshot.VisibleAsOfSeq}: page {pageNumber} was enumerated but TryGetValue no longer finds it (pass {i})");

                                        if (Describe(got) != expected)
                                            throw new InvalidOperationException($"snapshot seq {snapshot.VisibleAsOfSeq}: page {pageNumber} resolves to {Describe(got)}, was {expected} when the snapshot was taken (pass {i})");
                                    }

                                    Interlocked.Increment(ref passes);
                                    Thread.Yield();
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
                ctx.Fail(failure.ToString());
            else
                Console.WriteLine($"  clean: {commits} commits and {rebuilds} rebuilds under {snapshots} reader snapshots re-verified {passes} times");

            return ctx.Finish();
        }

        private static Dictionary<long, string> Materialize(ScratchPagesSnapshot snapshot)
        {
            var result = new Dictionary<long, string>(snapshot.Count);
            foreach (var (pageNumber, value) in snapshot)
            {
                if (result.TryAdd(pageNumber, Describe(value)) == false)
                    throw new InvalidOperationException($"snapshot seq {snapshot.VisibleAsOfSeq}: page {pageNumber} appears twice in one enumeration");
            }

            return result;
        }

        private static string Describe(in PageFromScratchBuffer value) =>
            $"{value.PositionInScratchBuffer}/{value.Size}/{value.NumberOfPages}/{value.AllocatedInTransaction}/{value.PageNumberInDataFile}";

        private static void Compare(long seq, Dictionary<long, string> first, Dictionary<long, string> again, int pass)
        {
            if (first.Count != again.Count)
            {
                var missing = first.Keys.Except(again.Keys).Take(5);
                var extra = again.Keys.Except(first.Keys).Take(5);
                throw new InvalidOperationException($"snapshot seq {seq} changed under a live read transaction: enumerated {again.Count} pages on pass {pass}, {first.Count} when taken (missing {string.Join(",", missing)}; extra {string.Join(",", extra)})");
            }

            foreach (var (pageNumber, expected) in first)
            {
                if (again.TryGetValue(pageNumber, out var got) == false)
                    throw new InvalidOperationException($"snapshot seq {seq} changed under a live read transaction: page {pageNumber} is gone on pass {pass}");

                if (got != expected)
                    throw new InvalidOperationException($"snapshot seq {seq} changed under a live read transaction: page {pageNumber} was {expected}, is {got} on pass {pass}");
            }
        }
    }
}
