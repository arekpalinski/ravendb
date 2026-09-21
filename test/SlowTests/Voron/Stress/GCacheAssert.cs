using System;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// g-cache-assert: adversarial traffic for the incremental collections cache (area g), on a Debug
    /// server where DocumentsStorage.AssertIncrementalCacheMatchesFullScan runs on EVERY commit - the
    /// author's own oracle. Two databases with bidirectional external replication and conflicts kept
    /// (ResolveToLatest = false); 4 workers do a weighted random mix of: puts across a growing set of
    /// collections (new ones created mid-run), updates, deletes, bulk inserts, attachments put/delete,
    /// counters, time series (incl. range deletes), forced revisions, patch-by-query, delete-by-query,
    /// HiLo puts, and deliberate cross-store collisions (same id written on both sides; delete on one
    /// side vs update on the other = conflict against a tombstone). After the main phase both databases
    /// are restarted and a short second phase runs on the re-seeded cache. Any server exception whose
    /// message contains the assert text fails the run; other client-visible errors (conflicts,
    /// concurrency) are expected and counted.
    /// </summary>
    public class GCacheAssert : ReplicationTestBase
    {
        private const string AssertText = "Incremental documents transaction cache disagrees";

        public GCacheAssert(ITestOutputHelper output) : base(output)
        {
        }

        public static int RunScenario(StressContext ctx) => RunScenarioAsync(ctx).GetAwaiter().GetResult();

        private static async Task<int> RunScenarioAsync(StressContext ctx)
        {
            using var output = new ConsoleTestOutputHelper();
            await using var test = new GCacheAssert(output);
            return await test.RunAsync(ctx);
        }

        private class Doc
        {
            public string Id;
            public string Name;
            public int Touched;
            public string Payload;
        }

        private readonly List<string> _collections = new() { "Orders", "Users", "Products", "Events", "Logs", "Sessions", "Items", "Notes" };
        private readonly object _collectionsLock = new();
        private long _newCollectionCounter;
        private long _opsDone;
        private long _expectedErrors;
        private string _assertFailure;
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _errorKinds = new();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> _errorSamples = new();

        private async Task<int> RunAsync(StressContext ctx)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            ctx.Iterations = 1;

            Options MakeOptions() => new()
            {
                ModifyDatabaseRecord = record => record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false }
            };

            using var storeA = GetDocumentStore(MakeOptions());
            using var storeB = GetDocumentStore(MakeOptions());

            await RevisionsHelper.SetupRevisionsAsync(storeA);
            await RevisionsHelper.SetupRevisionsAsync(storeB);
            await SetupReplicationAsync(storeA, storeB);
            await SetupReplicationAsync(storeB, storeA);

            Console.WriteLine($"databases up: {storeA.Database} <-> {storeB.Database} (conflicts kept, revisions on)");

            var mainPhase = TimeSpan.FromMinutes(ctx.Minutes * 0.8);
            RunWorkers(ctx, new[] { storeA, storeB }, mainPhase, "main");

            if (_assertFailure == null)
            {
                // restart both databases: the cache is re-seeded from a full scan and must agree from then on
                foreach (var store in new[] { storeA, storeB })
                {
                    store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                    store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                }

                Console.WriteLine("databases restarted; second phase");
                RunWorkers(ctx, new[] { storeA, storeB }, TimeSpan.FromMinutes(Math.Max(0.5, ctx.Minutes * 0.15)), "after-restart");
            }

            Console.WriteLine($"ops: {_opsDone}, expected client-visible errors (conflicts/concurrency/etc): {_expectedErrors}");
            Console.WriteLine($"reverts attempted: {_revertsAttempted}, completed: {_revertsCompleted}");
            foreach (var kind in System.Linq.Enumerable.Take(System.Linq.Enumerable.OrderByDescending(_errorKinds, k => k.Value), 8))
                Console.WriteLine($"    {kind.Value,7} x {kind.Key}");
            var samplesDir = Path.Combine(ctx.WorkDir, "error-samples");
            Directory.CreateDirectory(samplesDir);
            var n = 0;
            foreach (var kind in System.Linq.Enumerable.Take(System.Linq.Enumerable.OrderByDescending(_errorKinds, k => k.Value), 8))
                File.WriteAllText(Path.Combine(samplesDir, $"{n++:D2}-{kind.Value}.txt"), kind.Key + Environment.NewLine + Environment.NewLine + _errorSamples[kind.Key]);
            Console.WriteLine($"error samples written to {samplesDir}");

            if (_assertFailure != null)
                ctx.Fail(_assertFailure);

            return ctx.Finish();
        }

        private void RunWorkers(StressContext ctx, DocumentStore[] stores, TimeSpan duration, string phase)
        {
            using var stop = new CancellationTokenSource(duration);
            var token = stop.Token;
            var workers = new Thread[4];
            for (var w = 0; w < workers.Length; w++)
            {
                var worker = w;
                workers[w] = new Thread(() => WorkerLoop(ctx.Seed * 131 + worker + phase.Length, stores, worker, token, stop))
                {
                    IsBackground = true,
                    Name = $"g-cache-assert-{phase}-{worker}"
                };
                workers[w].Start();
            }

            var sp = System.Diagnostics.Stopwatch.StartNew();
            var lastReport = -1;
            while (token.IsCancellationRequested == false)
            {
                Thread.Sleep(1000);
                var slot = (int)(sp.Elapsed.TotalSeconds / 30);
                if (slot != lastReport)
                {
                    lastReport = slot;
                    Console.WriteLine($"  [{phase}] t+{sp.Elapsed.TotalSeconds:0}s: {Interlocked.Read(ref _opsDone)} ops, {Interlocked.Read(ref _expectedErrors)} expected errors, {_collections.Count} collections");
                }
            }

            foreach (var worker in workers)
                worker.Join();
        }

        private void WorkerLoop(int seed, DocumentStore[] stores, int worker, CancellationToken token, CancellationTokenSource stop)
        {
            var rng = new Random(seed);
            var baseline = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            while (token.IsCancellationRequested == false)
            {
                var store = stores[rng.Next(stores.Length)];
                var other = stores[(Array.IndexOf(stores, store) + 1) % stores.Length];
                var collection = PickCollection(rng);
                var id = $"{collection.ToLowerInvariant()}/{rng.Next(200)}";

                try
                {
                    switch (rng.Next(100))
                    {
                        case < 22:
                            Put(store, collection, id, rng);
                            break;
                        case < 32:
                            Delete(store, id);
                            break;
                        case < 38:
                            BulkInsert(store, collection, rng);
                            break;
                        case < 45:
                            Attachment(store, id, rng);
                            break;
                        case < 53:
                            Counters(store, id, rng);
                            break;
                        case < 62:
                            AppendOrDeleteTimeSeries(store, id, rng, baseline);
                            break;
                        case < 67:
                            ForceRevision(store, id);
                            break;
                        case < 71:
                            PatchByQuery(store, collection);
                            break;
                        case < 74:
                            DeleteByQuery(store, collection, rng);
                            break;
                        case < 79:
                            HiLoPut(store, collection, rng);
                            break;
                        case < 90:
                            // cross-store collision: both sides write the same id; one side may delete instead
                            Put(store, collection, id, rng);
                            if (rng.Next(3) == 0)
                                Delete(other, id);
                            else
                                Put(other, collection, id, rng);
                            break;
                        case < 94:
                            // smuggler: a bulk write path of its own, and the one most likely to reach the
                            // collection tables without going through the documents-transaction helpers
                            if (SmugglerEnabled)
                                SmugglerRoundTrip(store, other);
                            else
                                Touch(store, id);
                            break;
                        case < 97:
                            // revert writes documents and tombstones through the revisions machinery
                            if (RevertEnabled)
                                RevertToEarlier(store, rng);
                            else
                                Touch(store, id);
                            break;
                        default:
                            // update in place (touch)
                            Touch(store, id);
                            break;
                    }

                    Interlocked.Increment(ref _opsDone);
                }
                catch (Exception e) when (IsAssertFailure(e))
                {
                    Interlocked.CompareExchange(ref _assertFailure, $"worker {worker}: {e}", null);
                    stop.Cancel();
                    return;
                }
                catch (Exception e)
                {
                    // conflicts, concurrency, missing documents, index staleness timeouts: expected under this traffic
                    Interlocked.Increment(ref _expectedErrors);
                    var root = e;
                    while (root.InnerException != null)
                        root = root.InnerException;
                    var firstLine = root.Message.Split('\n')[0];
                    var kind = root.GetType().Name + ": " + (firstLine.Length > 90 ? firstLine[..90] : firstLine);
                    _errorKinds.AddOrUpdate(kind, 1, (_, c) => c + 1);
                    _errorSamples.TryAdd(kind, e.ToString());
                }
            }
        }

        // VORON_GCA_SMUGGLER=0 / VORON_GCA_REVERT=0 turn the two newer write paths off, to isolate which
        // of them is needed for finding s-13
        private static readonly bool SmugglerEnabled = Environment.GetEnvironmentVariable("VORON_GCA_SMUGGLER") != "0";
        private static readonly bool RevertEnabled = Environment.GetEnvironmentVariable("VORON_GCA_REVERT") != "0";

        private static void SmugglerRoundTrip(DocumentStore from, DocumentStore to)
        {
            var file = Path.Combine(Path.GetTempPath(), $"g-cache-assert-{Guid.NewGuid():N}.ravendbdump");
            try
            {
                var export = from.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file).GetAwaiter().GetResult();
                export.WaitForCompletionAsync(TimeSpan.FromMinutes(2)).GetAwaiter().GetResult();

                var import = to.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file).GetAwaiter().GetResult();
                import.WaitForCompletionAsync(TimeSpan.FromMinutes(2)).GetAwaiter().GetResult();
            }
            finally
            {
                try
                {
                    if (File.Exists(file))
                        File.Delete(file);
                }
                catch
                {
                    // the export may still hold it briefly; a stray temp file is not worth failing the run
                }
            }
        }

        private static long _revertsAttempted;
        private static long _revertsCompleted;

        private static void RevertToEarlier(DocumentStore store, Random rng)
        {
            Interlocked.Increment(ref _revertsAttempted);
            var operation = store.Maintenance.Send(new RevertRevisionsOperation(new RevertRevisionsRequest
            {
                Time = DateTime.UtcNow.AddSeconds(-rng.Next(5, 60)),
                WindowInSec = 120
            }));

            operation.WaitForCompletion(TimeSpan.FromMinutes(2));
            Interlocked.Increment(ref _revertsCompleted);
        }

        private static bool IsAssertFailure(Exception e)
        {
            for (var cur = e; cur != null; cur = cur.InnerException)
            {
                if (cur.Message.Contains(AssertText))
                    return true;
            }

            return e is AggregateException agg && agg.InnerExceptions.Count > 0 && IsAssertFailure(agg.InnerExceptions[0]);
        }

        private string PickCollection(Random rng)
        {
            lock (_collectionsLock)
            {
                if (rng.Next(200) == 0)
                {
                    var name = $"Dyn{Interlocked.Increment(ref _newCollectionCounter)}";
                    _collections.Add(name);
                    return name;
                }

                return _collections[rng.Next(_collections.Count)];
            }
        }

        private static void Put(DocumentStore store, string collection, string id, Random rng)
        {
            using var session = store.OpenSession();
            var doc = new Doc { Id = id, Name = "n" + rng.Next(1000), Payload = new string('p', rng.Next(2000)) };
            session.Store(doc, id);
            session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = collection;
            session.SaveChanges();
        }

        private static void Touch(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            var doc = session.Load<Doc>(id);
            if (doc == null)
                return;
            doc.Touched++;
            session.SaveChanges();
        }

        private static void Delete(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            session.Delete(id);
            session.SaveChanges();
        }

        private static void BulkInsert(DocumentStore store, string collection, Random rng)
        {
            using var bulk = store.BulkInsert();
            var count = 10 + rng.Next(40);
            var metadata = new MetadataAsDictionary { [Constants.Documents.Metadata.Collection] = collection };
            for (var i = 0; i < count; i++)
                bulk.Store(new Doc { Name = "bulk", Payload = new string('b', rng.Next(500)) }, $"{collection.ToLowerInvariant()}/{rng.Next(200)}", metadata);
        }

        private static void Attachment(DocumentStore store, string id, Random rng)
        {
            if (rng.Next(3) == 0)
            {
                store.Operations.Send(new DeleteAttachmentOperation(id, "a" + rng.Next(3)));
                return;
            }

            using var stream = new MemoryStream(Encoding.ASCII.GetBytes(new string('a', 1 + rng.Next(4000))));
            store.Operations.Send(new PutAttachmentOperation(id, "a" + rng.Next(3), stream, "text/plain"));
        }

        private static void Counters(DocumentStore store, string id, Random rng)
        {
            using var session = store.OpenSession();
            var counters = session.CountersFor(id);
            if (rng.Next(4) == 0)
                counters.Delete("c" + rng.Next(3));
            else
                counters.Increment("c" + rng.Next(3), rng.Next(10));
            session.SaveChanges();
        }

        private static void AppendOrDeleteTimeSeries(DocumentStore store, string id, Random rng, DateTime baseline)
        {
            using var session = store.OpenSession();
            var ts = session.TimeSeriesFor(id, "HeartRate");
            if (rng.Next(4) == 0)
            {
                var from = baseline.AddMinutes(rng.Next(500));
                ts.Delete(from, from.AddMinutes(rng.Next(50)));
            }
            else
            {
                var start = rng.Next(1000);
                for (var i = 0; i < 1 + rng.Next(10); i++)
                    ts.Append(baseline.AddMinutes(start + i), rng.Next(200), "w");
            }

            session.SaveChanges();
        }

        private static void ForceRevision(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            session.Advanced.Revisions.ForceRevisionCreationFor(id);
            session.SaveChanges();
        }

        private static void PatchByQuery(DocumentStore store, string collection)
        {
            var op = store.Operations.Send(new PatchByQueryOperation($"from {collection} update {{ this.Touched = (this.Touched || 0) + 1; }}"));
            op.WaitForCompletion(TimeSpan.FromSeconds(30));
        }

        private static void DeleteByQuery(DocumentStore store, string collection, Random rng)
        {
            var op = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = $"from {collection} where Name = 'n{rng.Next(1000)}'" }));
            op.WaitForCompletion(TimeSpan.FromSeconds(30));
        }

        private static void HiLoPut(DocumentStore store, string collection, Random rng)
        {
            using var session = store.OpenSession();
            var doc = new Doc { Name = "hilo", Payload = new string('h', rng.Next(300)) };
            session.Store(doc);
            session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = collection;
            session.SaveChanges();
        }
    }
}
