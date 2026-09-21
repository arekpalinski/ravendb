using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// g-wakeup-watchdog: the split after-commit notifications (one internal wakeup per changed
    /// collection for documents, per transaction for counters / time series, replication-only bits
    /// per domain) must never lose a wakeup. Store A replicates to store B, so B sees every write from
    /// A as an INCOMING-REPLICATION-ONLY transaction - the case where the aggregation had already
    /// broken counter/time-series index wakeups once. B carries an auto index over documents, a static
    /// index over counters and one over time series. Workers write marked documents, counters and
    /// time-series entries to A (replicated) or directly to B, and delete documents; a watchdog then
    /// queries the matching index on B for the marker with WaitForNonStaleResults(45s). A timeout is
    /// a lost wakeup (the index never learned about the write). Deletes are checked the same way
    /// (the marker must disappear).
    /// </summary>
    public class GWakeupWatchdog : ReplicationTestBase
    {
        private const string Collection = "Users";
        private const int Deadline = 45_000;

        public GWakeupWatchdog(ITestOutputHelper output) : base(output)
        {
        }

        public static int RunScenario(StressContext ctx) => RunScenarioAsync(ctx).GetAwaiter().GetResult();

        private static async Task<int> RunScenarioAsync(StressContext ctx)
        {
            using var output = new ConsoleTestOutputHelper();
            await using var test = new GWakeupWatchdog(output);
            return await test.RunAsync(ctx);
        }

        private class User
        {
            public string Id;
            public string Marker;
            public int Touched;
        }

        private class CounterIndexResult
        {
            public string DocumentId;
            public string CounterName;
            public long Value;
        }

        private class TimeSeriesIndexResult
        {
            public string DocumentId;
            public string Tag;
            public double Value;
        }

        private long _checks, _lost, _transient;
        private readonly ThreadLocal<string> _lastTsStats = new();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _missKinds = new();
        private readonly List<string> _failures = new();

        private async Task<int> RunAsync(StressContext ctx)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            ctx.Iterations = 1;

            using var storeA = GetDocumentStore();
            using var storeB = GetDocumentStore();

            storeB.Maintenance.Send(new PutIndexesOperation(
                new IndexDefinition
                {
                    Name = "Counters/ByMarker",
                    Maps = { "from counter in counters.Users select new { DocumentId = counter.DocumentId, CounterName = counter.Name, Value = counter.Value }" }
                },
                new IndexDefinition
                {
                    Name = "TimeSeries/ByTag",
                    Maps = { "from ts in timeSeries.Users.HeartRate from entry in ts.Entries select new { DocumentId = ts.DocumentId, Tag = entry.Tag, Value = entry.Value }" }
                }));

            await SetupReplicationAsync(storeA, storeB);

            // seed a few documents on both sides so counters / time series have owners
            foreach (var store in new[] { storeA, storeB })
            {
                using var session = store.OpenSession();
                for (var i = 0; i < 50; i++)
                    session.Store(new User { Marker = "seed" }, $"users/{i}");
                session.SaveChanges();
            }

            await EnsureReplicatingAsync(storeA, storeB);
            Console.WriteLine($"stores up: {storeA.Database} -> {storeB.Database}; indexes on B: auto (docs), Counters/ByMarker, TimeSeries/ByTag");

            using var stop = new CancellationTokenSource(TimeSpan.FromMinutes(ctx.Minutes));
            var token = stop.Token;
            var workers = new Thread[4];
            for (var w = 0; w < workers.Length; w++)
            {
                var worker = w;
                workers[w] = new Thread(() => WorkerLoop(ctx.Seed * 977 + worker, storeA, storeB, worker, token)) { IsBackground = true };
                workers[w].Start();
            }

            var sp = Stopwatch.StartNew();
            var lastReport = -1;
            while (token.IsCancellationRequested == false && Volatile.Read(ref _lost) == 0)
            {
                Thread.Sleep(1000);
                var slot = (int)(sp.Elapsed.TotalSeconds / 30);
                if (slot != lastReport)
                {
                    lastReport = slot;
                    Console.WriteLine($"  t+{sp.Elapsed.TotalSeconds:0}s: {Interlocked.Read(ref _checks)} wakeups verified, {Interlocked.Read(ref _lost)} lost, {Interlocked.Read(ref _transient)} transient misses");
                }
            }

            stop.Cancel();
            foreach (var worker in workers)
                worker.Join();

            Console.WriteLine($"  {_checks} wakeups verified, {_lost} lost, {_transient} transient misses");
            foreach (var kv in _missKinds.OrderByDescending(k => k.Value))
                Console.WriteLine($"    {kv.Value,6} x {kv.Key}");
            lock (_failures)
            {
                // a genuine lost wakeup fails the run; transient misses are reported as a finding-in-progress
                foreach (var f in _failures.Where(f => f.StartsWith("LOST") || f.StartsWith("REPLICATION")).Take(5))
                    ctx.Fail(f);
                foreach (var f in _failures.Where(f => f.StartsWith("TRANSIENT")).Take(3))
                    Console.WriteLine("  " + f);
            }
            return ctx.Finish();
        }

        private void WorkerLoop(int seed, DocumentStore storeA, DocumentStore storeB, int worker, CancellationToken token)
        {
            var rng = new Random(seed);
            var baseline = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var n = 0;

            while (token.IsCancellationRequested == false)
            {
                n++;
                var marker = $"w{worker}-{n}-{rng.Next(1_000_000)}";
                var viaReplication = worker % 2 == 0; // even workers write to A (B sees replication-only txs), odd workers write directly to B
                var writeStore = viaReplication ? storeA : storeB;
                var id = $"users/{worker}-{rng.Next(30)}"; // per-worker ids: no cross-worker overwrites, no cross-store conflicts
                var kind = rng.Next(4);

                try
                {
                    switch (kind)
                    {
                        case 0: // document put -> auto index on B must see the marker
                        {
                            using (var session = writeStore.OpenSession())
                            {
                                var user = session.Load<User>(id) ?? new User();
                                user.Marker = marker;
                                user.Touched++;
                                session.Store(user, id);
                                session.SaveChanges();
                            }

                            if (WaitOnB(storeB, viaReplication, marker, "document", s => s.Load<User>(id)?.Marker == marker) == false)
                                break;
                            Check(storeB, marker, viaReplication, "document", s =>
                                s.Query<User>().Customize(c => c.WaitForNonStaleResults(TimeSpan.FromMilliseconds(Deadline))).Where(u => u.Marker == marker).Count() == 1);
                            break;
                        }
                        case 1: // counter -> Counters/ByMarker must see the counter named after the marker
                        {
                            using (var session = writeStore.OpenSession())
                            {
                                if (session.Load<User>(id) == null)
                                    session.Store(new User { Marker = "owner" }, id);
                                session.CountersFor(id).Increment(marker, 1);
                                session.SaveChanges();
                            }

                            if (WaitOnB(storeB, viaReplication, marker, "counter", s => s.CountersFor(id).Get(marker) != null) == false)
                                break;
                            Check(storeB, marker, viaReplication, "counter", s =>
                                s.Query<CounterIndexResult>("Counters/ByMarker").Customize(c => c.WaitForNonStaleResults(TimeSpan.FromMilliseconds(Deadline))).Where(r => r.CounterName == marker).Count() == 1,
                                s =>
                                {
                                    var onB = s.CountersFor(id).GetAll()?.Keys.ToList() ?? new List<string>();
                                    var indexed = s.Query<CounterIndexResult>("Counters/ByMarker").Where(r => r.DocumentId == id).Select(r => r.CounterName).ToList();
                                    return $"doc {id}: counters on B = [{string.Join(",", onB)}]; indexed counters for doc = [{string.Join(",", indexed)}]";
                                });
                            break;
                        }
                        case 2: // time series entry tagged with the marker -> TimeSeries/ByTag must see it
                        {
                            var at = baseline.AddSeconds(n * 1000 + worker);
                            using (var session = writeStore.OpenSession())
                            {
                                if (session.Load<User>(id) == null)
                                    session.Store(new User { Marker = "owner" }, id);
                                session.TimeSeriesFor(id, "HeartRate").Append(at, rng.Next(200), marker);
                                session.SaveChanges();
                            }

                            if (WaitOnB(storeB, viaReplication, marker, "time series", s => s.TimeSeriesFor(id, "HeartRate").Get(at, at)?.Any(e => e.Tag == marker) == true) == false)
                                break;
                            Check(storeB, marker, viaReplication, "time series", s =>
                                {
                                    var count = s.Query<TimeSeriesIndexResult>("TimeSeries/ByTag").Statistics(out var st).Customize(c => c.WaitForNonStaleResults(TimeSpan.FromMilliseconds(Deadline))).Where(r => r.Tag == marker).Count();
                                    if (count != 1)
                                        _lastTsStats.Value = $"server said IsStale={st.IsStale}, ResultEtag={st.ResultEtag}, IndexTimestamp={st.IndexTimestamp:HH:mm:ss.fff}, took {st.DurationInMs}ms, TotalResults={st.TotalResults}";
                                    return count == 1;
                                },
                                s =>
                                {
                                    var onB = s.TimeSeriesFor(id, "HeartRate").Get()?.Select(e => $"{e.Timestamp:HH:mm:ss}:{e.Tag}").ToList() ?? new List<string>();
                                    var indexed = s.Query<TimeSeriesIndexResult>("TimeSeries/ByTag").Where(r => r.DocumentId == id).Select(r => r.Tag).ToList();
                                    return $"{_lastTsStats.Value}; doc {id}: ts entries on B = [{string.Join(",", onB)}]; indexed tags for doc = [{string.Join(",", indexed)}]";
                                });
                            break;
                        }
                        default: // delete-only transaction -> the document's previous marker must disappear from the auto index
                        {
                            string previous;
                            using (var session = writeStore.OpenSession())
                            {
                                var user = session.Load<User>(id);
                                if (user == null)
                                    continue;
                                previous = user.Marker;
                                session.Delete(id);
                                session.SaveChanges();
                            }

                            if (WaitOnB(storeB, viaReplication, previous, "delete", s => s.Load<User>(id) == null) == false)
                                break;
                            Check(storeB, previous, viaReplication, "delete", s =>
                                s.Query<User>().Customize(c => c.WaitForNonStaleResults(TimeSpan.FromMilliseconds(Deadline))).Where(u => u.Id == id).Count() == 0);

                            // put it back so counters / time series keep an owner
                            using (var session = writeStore.OpenSession())
                            {
                                session.Store(new User { Marker = "restored" }, id);
                                session.SaveChanges();
                            }
                            break;
                        }
                    }
                }
                catch (Exception e) when (e is not LostWakeup)
                {
                    // conflicts, missing docs, concurrent restores: not what we measure here
                }
            }
        }

        private sealed class LostWakeup : Exception
        {
        }

        // for writes that travel through replication, wait until B actually holds the data (direct reads, no index) before
        // asking the index about it - WaitForNonStaleResults only covers what B already received
        private bool WaitOnB(DocumentStore storeB, bool viaReplication, string marker, string what, Func<Raven.Client.Documents.Session.IDocumentSession, bool> arrived)
        {
            if (viaReplication == false)
                return true;

            var sp = Stopwatch.StartNew();
            while (sp.ElapsedMilliseconds < Deadline)
            {
                try
                {
                    using var session = storeB.OpenSession();
                    if (arrived(session))
                        return true;
                }
                catch
                {
                    // transient (e.g. conflict while resolving) - keep polling
                }

                Thread.Sleep(50);
            }

            Record($"REPLICATION LAG: {what} marker '{marker}' did not reach B within {Deadline / 1000}s (not an index wakeup problem)");
            return false;
        }

        private void Check(DocumentStore storeB, string marker, bool viaReplication, string what, Func<Raven.Client.Documents.Session.IDocumentSession, bool> query,
            Func<Raven.Client.Documents.Session.IDocumentSession, string> diagnose = null)
        {
            Interlocked.Increment(ref _checks);
            try
            {
                using var session = storeB.OpenSession();
                if (query(session))
                    return;

                // one retry after a short pause: distinguishes a transient (index still applying the last batch) from a genuinely lost update
                Thread.Sleep(2000);
                using var retry = storeB.OpenSession();
                var stillMissing = query(retry) == false;
                var kind = $"{what} {(viaReplication ? "via replication" : "direct")} {(stillMissing ? "LOST" : "TRANSIENT")}";
                _missKinds.AddOrUpdate(kind, 1, (_, c) => c + 1);
                if (stillMissing)
                    Record($"LOST WAKEUP: {what} marker '{marker}' ({(viaReplication ? "via replication" : "direct")}) - index reported non-stale, the query did not reflect it and still does not 2s later. {Diagnostics(storeB, retry, diagnose)}");
                else
                {
                    Interlocked.Increment(ref _transient);
                    if (Interlocked.Read(ref _transient) <= 3)
                        lock (_failures)
                            _failures.Add($"TRANSIENT (visible 2s later): {what} marker '{marker}' ({(viaReplication ? "via replication" : "direct")}) - index reported non-stale but the query did not reflect it. {Diagnostics(storeB, retry, diagnose)}");
                }
            }
            catch (Exception e) when (e.Message.Contains("stale", StringComparison.OrdinalIgnoreCase) || e.GetType().Name.Contains("Timeout"))
            {
                Record($"LOST WAKEUP: {what} marker '{marker}' ({(viaReplication ? "via replication" : "direct")}) was not indexed on B within {Deadline / 1000}s: {e.GetType().Name}: {Truncate(e.Message)}");
            }
        }

        private static string Diagnostics(DocumentStore storeB, Raven.Client.Documents.Session.IDocumentSession session, Func<Raven.Client.Documents.Session.IDocumentSession, string> diagnose)
        {
            try
            {
                var errors = storeB.Maintenance.Send(new GetIndexErrorsOperation());
                var errorText = string.Join(" | ", errors.Where(e => e.Errors.Length > 0).Select(e => $"{e.Name}: {e.Errors.Length} errors, first: {e.Errors[0].Error?.Split(new[] { (char)10 })[0]}"));
                var stats = storeB.Maintenance.Send(new GetIndexesStatisticsOperation());
                var statText = string.Join(" | ", stats.Select(x => $"{x.Name}: stale={x.IsStale} state={x.State} entries={x.EntriesCount}"));
                var detail = diagnose?.Invoke(session);
                return $"DIAG index errors [{errorText}] stats [{statText}] detail [{detail}]";
            }
            catch (Exception e)
            {
                return $"DIAG failed: {e.GetType().Name}: {Truncate(e.Message)}";
            }
        }

        private void Record(string message)
        {
            Interlocked.Increment(ref _lost);
            lock (_failures)
                _failures.Add(message);
        }

        private static string Truncate(string s)
        {
            var line = s.Split('\n')[0];
            return line.Length > 160 ? line[..160] : line;
        }
    }
}
