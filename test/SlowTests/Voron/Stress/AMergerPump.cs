using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// a-merger-pump: the ack-driven AsyncCommitCompletionPump under failure. Per iteration a fresh
    /// database gets slow journal writes (so the merger's async-commit chains actually pipeline) and a
    /// single injected stage-2 failure (SimulateThrowingOnCommitStage2 on the N-th transaction, N seeded)
    /// while 8 client workers saturate the transaction merger. Oracles:
    ///   1. no hung waiter - every SaveChanges either succeeds or throws within 30s,
    ///   2. after the failure the database keeps answering (success or a prompt error, never a hang),
    ///   3. disposing/deleting the database with a chain in flight completes within 60s,
    ///   4. no Debug.Assert of the pump surfaces in any error text (Debug build).
    /// </summary>
    public class AMergerPump : RavenTestBase
    {
        private const int Workers = 8;
        private const int OpTimeoutMs = 30_000;
        private const int DisposeTimeoutMs = 60_000;

        public AMergerPump(ITestOutputHelper output) : base(output)
        {
        }

        public static int RunScenario(StressContext ctx) => RunScenarioAsync(ctx).GetAwaiter().GetResult();

        private static async Task<int> RunScenarioAsync(StressContext ctx)
        {
            using var output = new ConsoleTestOutputHelper();
            await using var test = new AMergerPump(output);
            return await test.RunAsync(ctx);
        }

        private class Doc
        {
            public string Id;
            public string Payload;
        }

        private async Task<int> RunAsync(StressContext ctx)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var rng = new Random(ctx.Seed);

            for (var iteration = 0; ctx.TimeLeft; iteration++)
            {
                ctx.Iteration = iteration;
                ctx.Iterations++;
                await RunIteration(ctx, rng, iteration);
                if (ctx.Failures.Count > 0)
                    break;
            }

            return ctx.Finish();
        }

        private async Task RunIteration(StressContext ctx, Random rng, int iteration)
        {
            var failAt = 40 + rng.Next(400);
            var journalSleepMs = 1 + rng.Next(5);
            var seen = 0L;
            var injected = new ManualResetEventSlim(false);

            var store = GetDocumentStore();
            var disposeMs = -1L;
            try
            {
                var db = await GetDocumentDatabaseInstanceFor(store);
                var env = db.DocumentsStorage.Environment;
                env.Options.ForTestingPurposesOnly().OnJournalWrite = (_, _) => Thread.Sleep(journalSleepMs);
                env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = tt =>
                {
                    if (Interlocked.Increment(ref seen) == failAt)
                    {
                        tt.SimulateThrowingOnCommitStage2 = true;
                        injected.Set();
                    }
                };

                long ok = 0, failed = 0;
                var errorKinds = new ConcurrentDictionary<string, long>();
                string hang = null;
                string assertText = null;

                using var stop = new CancellationTokenSource();
                var workers = new Thread[Workers];
                for (var w = 0; w < workers.Length; w++)
                {
                    var wrng = new Random(rng.Next());
                    workers[w] = new Thread(() =>
                    {
                        while (stop.IsCancellationRequested == false)
                        {
                            var task = Task.Run(() =>
                            {
                                using var session = store.OpenSession();
                                var n = 3 + wrng.Next(8);
                                var first = wrng.Next(5000);
                                for (var i = 0; i < n; i++)
                                    session.Store(new Doc { Payload = new string('p', wrng.Next(3000)) }, $"docs/{(first + i) % 5000}"); // distinct ids within one session
                                session.SaveChanges();
                            });

                            bool completed;
                            try
                            {
                                completed = task.Wait(OpTimeoutMs);
                            }
                            catch (AggregateException)
                            {
                                completed = true; // faulted - classified below
                            }

                            if (completed == false)
                            {
                                Interlocked.CompareExchange(ref hang, $"a SaveChanges did not complete within {OpTimeoutMs / 1000}s (injected={injected.IsSet}, ok={Interlocked.Read(ref ok)}, failed={Interlocked.Read(ref failed)})", null);
                                stop.Cancel();
                                return;
                            }

                            if (task.IsFaulted)
                            {
                                Interlocked.Increment(ref failed);
                                var root = task.Exception.GetBaseException();
                                var kind = root.GetType().Name + ": " + Truncate(root.Message);
                                errorKinds.AddOrUpdate(kind, 1, (_, c) => c + 1);
                                var text = root.ToString();
                                if (text.Contains("Assertion") || text.Contains("Debug.Assert") || text.Contains("every async committed transaction"))
                                    Interlocked.CompareExchange(ref assertText, text, null);
                            }
                            else
                            {
                                Interlocked.Increment(ref ok);
                            }
                        }
                    })
                    { IsBackground = true, Name = $"a-merger-pump-{iteration}-{w}" };
                    workers[w].Start();
                }

                // run until the injection lands (or 40s), then keep hammering the failed environment a bit longer
                var sp = Stopwatch.StartNew();
                while (injected.IsSet == false && sp.Elapsed.TotalSeconds < 40 && stop.IsCancellationRequested == false)
                    Thread.Sleep(50);
                var injectedAt = sp.Elapsed;
                Thread.Sleep(5000);
                stop.Cancel();
                foreach (var worker in workers)
                    worker.Join();

                if (hang != null)
                {
                    ctx.Fail($"iteration {iteration} (failAt {failAt}, journal sleep {journalSleepMs}ms): HUNG WAITER - {hang}");
                    return;
                }

                if (assertText != null)
                {
                    ctx.Fail($"iteration {iteration}: a pump assertion surfaced to a client: {assertText}");
                    return;
                }

                // dispose with whatever the merger still has in flight; RavenTestBase deletes the database here
                var dsp = Stopwatch.StartNew();
                var disposeTask = Task.Run(() => store.Dispose());
                if (disposeTask.Wait(DisposeTimeoutMs) == false)
                {
                    ctx.Fail($"iteration {iteration}: database dispose/delete did not complete within {DisposeTimeoutMs / 1000}s after the injected failure (failAt {failAt})");
                    return;
                }
                disposeMs = dsp.ElapsedMilliseconds;
                store = null;

                var top = new List<string>();
                foreach (var kv in errorKinds)
                    top.Add($"{kv.Value}x {kv.Key}");
                Console.WriteLine($"  iter {iteration}: failAt {failAt} (injected after {injectedAt.TotalSeconds:0.0}s, journal sleep {journalSleepMs}ms), ok {ok}, failed {failed}, dispose {disposeMs}ms; errors: {string.Join(" | ", top)}");
            }
            finally
            {
                store?.Dispose();
            }
        }

        private static string Truncate(string s)
        {
            var line = s.Split('\n')[0];
            return line.Length > 100 ? line[..100] : line;
        }
    }
}
