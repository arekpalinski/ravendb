using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Voron;
using Voron.Impl;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// a-fault-matrix: a partial journal-write failure (0 / 1 / half / all-but-one 4KB blocks
    /// actually written) is injected into a randomly chosen write while several are in flight.
    /// Expectations: the environment turns catastrophic; every transaction whose EndAsyncCommit
    /// returned successfully BEFORE the failure surfaced must survive the restart; no transaction
    /// that failed (or was never completed) may be readable after recovery; recovery itself either
    /// opens cleanly (truncating at the hole) or refuses loudly - it must never open and serve a
    /// transaction from beyond the hole.
    /// </summary>
    public static class AFaultMatrix
    {
        private const int PipelineDepth = 4;
        private const int InFlightWindow = 4;

        public static int Run(StressContext ctx)
        {
            var partials = new Func<long, long>[]
            {
                _ => 0, // nothing hits the disk
                _ => 1, // torn head
                total => total / 2, // torn middle
                total => Math.Max(0, total - 1) // torn tail
            };

            var rng = new Random(ctx.Seed);
            for (var iteration = 0; ctx.TimeLeft; iteration++)
            {
                ctx.Iteration = iteration;
                ctx.Iterations++;
                var dir = ctx.IterationDir(iteration);
                Directory.CreateDirectory(dir);

                try
                {
                    RunIteration(ctx, dir, rng, partials[iteration % partials.Length]);
                }
                catch (Exception e)
                {
                    ctx.Fail($"iteration threw outside the expected failure surface: {e}");
                }

                if (ctx.Failures.Count > 0)
                    break;

                Directory.Delete(dir, recursive: true);
            }

            return ctx.Finish();
        }

        private static void RunIteration(StressContext ctx, string dir, Random rng, Func<long, long> partialOf)
        {
            var failAtWrite = 3 + rng.Next(28);
            var chainLength = failAtWrite + 10 + rng.Next(20);
            var delayMaxMs = 1 + rng.Next(25);

            var acked = new List<long>(); // tx ids whose EndAsyncCommit returned success
            var failed = new List<long>(); // tx ids whose completion threw
            var wrote = new HashSet<long>(); // tx ids whose WriteTx completed - only those carry a verifiable key
            var ackedVia = new Dictionary<long, string>();
            var sawInjectedFailure = false;

            using (var env = new StorageEnvironment(CreateOptions(dir)))
            {
                var writesSeen = 0;
                var iterationSeed = rng.Next();
                env.Options.ForTestingPurposesOnly().OnJournalWrite = (posBy4Kb, _) =>
                    Thread.Sleep((int)(unchecked((ulong)(posBy4Kb * 2654435761L + iterationSeed)) % (uint)delayMaxMs));
                env.Options.ForTestingPurposesOnly().SimulatePartialJournalWriteFailure = total =>
                {
                    if (Interlocked.Increment(ref writesSeen) != failAtWrite)
                        return null;

                    return new StorageEnvironmentOptions.TestingStuff.PartialJournalWriteFailure
                    {
                        NumberOf4KbsToWrite = partialOf(total),
                        Error = new IOException("a-fault-matrix injected journal write failure")
                    };
                };

                var context = new TransactionPersistentContext(true);
                var inFlight = new Queue<Transaction>();
                Transaction previous = null;

                try
                {
                    previous = env.WriteTransaction(context);
                    WriteTx(previous);
                    wrote.Add(previous.LowLevelTransaction.Id);

                    for (var i = 1; i < chainLength; i++)
                    {
                        var current = previous.BeginAsyncCommitAndStartNewTransaction(context);
                        inFlight.Enqueue(previous);
                        previous = current; // keep `previous` = the un-enqueued tail at every point, so the cleanup below never leaks it

                        WriteTx(previous);
                        wrote.Add(previous.LowLevelTransaction.Id);

                        while (inFlight.Count >= InFlightWindow)
                            CompleteOldest(inFlight, acked, failed, ackedVia, ref sawInjectedFailure);
                    }

                    while (inFlight.Count > 0)
                        CompleteOldest(inFlight, acked, failed, ackedVia, ref sawInjectedFailure);

                    previous.Commit();
                    ackedVia[previous.LowLevelTransaction.Id] = "final-sync-commit";
                    acked.Add(previous.LowLevelTransaction.Id);
                    previous.Dispose();
                    previous = null;
                }
                catch (Exception)
                {
                    sawInjectedFailure = true;

                    // mirror the merger's failure path: drain the in-flight chain in FIFO order first,
                    // then complete the tail (it holds the write lock - leaving it open keeps the lock forever)
                    while (inFlight.Count > 0)
                    {
                        // a tx sequenced BEFORE the failed write completes durably even though the
                        // chain as a whole failed - classify by the actual EndAsyncCommit outcome
                        var tx = inFlight.Dequeue();
                        var id = tx.LowLevelTransaction.Id;
                        try
                        {
                            tx.EndAsyncCommit();
                            ackedVia[id] = "cleanup-drain-EndAsyncCommit";
                            acked.Add(id);
                        }
                        catch
                        {
                            failed.Add(id);
                        }
                        finally
                        {
                            try { tx.Dispose(); } catch { }
                        }
                    }

                    if (previous != null)
                    {
                        var id = previous.LowLevelTransaction.Id;
                        try
                        {
                            previous.Commit();
                            ackedVia[id] = "cleanup-tail-sync-Commit";
                            acked.Add(id);
                        }
                        catch
                        {
                            failed.Add(id);
                        }
                        finally
                        {
                            try { previous.Dispose(); } catch { }
                        }
                    }
                }

                if (sawInjectedFailure == false)
                {
                    ctx.Fail($"the injected failure at write {failAtWrite} never surfaced to any caller ({acked.Count} acked txs)");
                    return;
                }

                var catastrophic = false;
                try
                {
                    env.Options.AssertNoCatastrophicFailure();
                }
                catch
                {
                    catastrophic = true;
                }

                if (catastrophic == false)
                    ctx.Fail("a lost journal write did not mark the environment as catastrophically failed");
            }

            if (ctx.Failures.Count > 0)
                return;

            // recovery contract: the recovered transactions must form a GAPLESS PREFIX of the chain
            // that covers every acknowledged tx. A failed tx re-appearing is acceptable ambiguity
            // (its write may have physically completed before the error was reported) - but only as
            // part of that prefix, never beyond a hole.
            try
            {
                using var env = new StorageEnvironment(CreateOptions(dir));
                using var tx = env.ReadTransaction();
                var tree = tx.ReadTree("data");

                var all = new List<long>(acked.Count + failed.Count);
                all.AddRange(acked.FindAll(wrote.Contains));
                all.AddRange(failed.FindAll(wrote.Contains));
                all.Sort();

                var firstMissing = long.MaxValue;
                foreach (var id in all)
                {
                    var present = tree?.Read(KeyOf(id)) != null;
                    if (present == false)
                    {
                        firstMissing = Math.Min(firstMissing, id);
                        continue;
                    }

                    if (id > firstMissing)
                    {
                        ctx.Fail($"tx {id} is readable after recovery although tx {firstMissing} is missing - a transaction beyond the hole was served (fail-at-write {failAtWrite})");
                        return;
                    }
                }

                foreach (var id in acked.FindAll(wrote.Contains))
                {
                    if (id >= firstMissing)
                    {
                        ctx.Fail($"tx {id} was acknowledged durable (via {ackedVia.GetValueOrDefault(id, "?")}) but is missing after recovery (first missing {firstMissing}, fail-at-write {failAtWrite})");
                        return;
                    }
                }

                var resurrected = failed.FindAll(id => id < firstMissing).Count;
                Console.WriteLine($"  iter {ctx.Iteration}: fail@{failAtWrite}, {acked.Count} acked survived, {failed.Count} failed ({resurrected} landed as ambiguous-but-prefix), prefix intact");
            }
            catch (Exception)
            {
                // refusing to open after a torn write is a legitimate, fail-loud outcome
                Console.WriteLine($"  iter {ctx.Iteration}: fail@{failAtWrite}, recovery refused to open (fail-loud) - accepted");
            }
        }

        private static void CompleteOldest(Queue<Transaction> inFlight, List<long> acked, List<long> failed, Dictionary<long, string> ackedVia, ref bool sawFailure)
        {
            var oldest = inFlight.Dequeue();
            var id = oldest.LowLevelTransaction.Id;
            try
            {
                oldest.EndAsyncCommit();
                ackedVia[id] = "in-loop-EndAsyncCommit";
                acked.Add(id);
            }
            catch
            {
                failed.Add(id);
                sawFailure = true;
                throw;
            }
            finally
            {
                try { oldest.Dispose(); } catch { }
            }
        }

        private static void WriteTx(Transaction tx)
        {
            var id = tx.LowLevelTransaction.Id;
            tx.CreateTree("data").Add(KeyOf(id), Encoding.ASCII.GetBytes($"tx-{id}-payload-{new string('x', 256)}"));
        }

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxConcurrentJournalWrites = PipelineDepth;
            options.PipelineJournalWritesAboveLatencyInTicks = 0;
            return options;
        }

        private static string KeyOf(long txId) => $"tx/{txId:D10}";
    }
}
