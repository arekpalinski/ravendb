using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// g-etag-restart: the per-collection etag cache the PR added (DocumentTransactionCache.CollectionCache, recomputed
    /// incrementally for the collections a transaction touched) against the full-scan truth, in a RELEASE build where the
    /// server's own per-commit assert (DocumentsStorage.AssertIncrementalCacheMatchesFullScan) is compiled out.
    ///
    /// Traffic aims at the shapes the incremental path special-cases: a collection created and written in the same
    /// transaction (its CollectionName still has Index -1 during that transaction and is only picked up by the
    /// count-based detection at commit), several collections created concurrently (index assigned by enumeration order,
    /// not creation order), a whole collection emptied by delete-by-query (LastDocumentEtag goes back to 0), and
    /// tombstone cleanup (LastTombstoneEtag goes back too), mixed with puts, deletes, bulk inserts and patch-by-query
    /// - the last two read GetLastDocumentEtag(collection) as their upper bound.
    ///
    /// Oracle: every two seconds the traffic is paused, in-flight operations drain, and every collection's
    /// last document / tombstone etag served by a read transaction (cache) is compared with the same values read in a
    /// write transaction (storage, the cache is bypassed), plus the collection sets of both. At the end the database is
    /// restarted (the cache is re-seeded by a full scan) and the served values must equal the ones before the restart.
    /// </summary>
    public class GEtagRestart(ITestOutputHelper output) : RavenTestBase(output)
    {
        public static int RunScenario(StressContext ctx) => RunScenarioAsync(ctx).GetAwaiter().GetResult();

        private static async Task<int> RunScenarioAsync(StressContext ctx)
        {
            using var output = new ConsoleTestOutputHelper();
            await using var test = new GEtagRestart(output);
            return await test.RunAsync(ctx);
        }

        private class Doc
        {
            public string Name;
            public int Touched;
            public string Payload;
        }

        private readonly List<string> _collections = Enumerable.Range(0, 30).Select(i => $"C{i:D2}").ToList();
        private readonly object _collectionsLock = new();
        private readonly ReaderWriterLockSlim _gate = new(LockRecursionPolicy.NoRecursion);
        private readonly ConcurrentDictionary<string, long> _ops = new();
        private readonly ConcurrentDictionary<string, long> _errors = new();
        private long _newCollections;
        private long _checks;
        private long _lagRetries;

        private async Task<int> RunAsync(StressContext ctx)
        {
            ctx.Iterations = 1;
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            using var store = GetDocumentStore(new Options { RunInMemory = false });

            var mainPhase = TimeSpan.FromMinutes(ctx.Minutes * 0.85);
            await RunTraffic(ctx, store, mainPhase, "main");
            if (ctx.Failures.Count > 0)
                return Report(ctx);

            // the last transaction before the restart does the special shapes at once: two collections created and
            // written in the same transaction, and every document of an existing collection deleted
            await FinalTransaction(store);
            var database = await GetDatabase(store.Database);
            await database.TombstoneCleaner.ExecuteCleanup();

            if (Check(ctx, database, "before restart") == false)
                return Report(ctx);

            var before = Served(database);
            Console.WriteLine($"restarting the database ({before.Count} collections)");
            await Server.ServerStore.DatabasesLandlord.RestartDatabaseAsync(store.Database);
            database = await GetDatabase(store.Database);

            var after = Served(database);
            foreach (var (collection, value) in before)
            {
                if (after.TryGetValue(collection, out var reloaded) == false)
                    ctx.Fail($"collection '{collection}' is served before the restart ({value}) but not after it");
                else if (reloaded != value)
                    ctx.Fail($"collection '{collection}': served {value} before the restart, {reloaded} after it (the re-seeded cache disagrees with the incremental one)");
            }

            foreach (var collection in after.Keys.Except(before.Keys))
                ctx.Fail($"collection '{collection}' appeared only after the restart ({after[collection]})");

            if (ctx.Failures.Count == 0 && Check(ctx, database, "after restart"))
                await RunTraffic(ctx, store, TimeSpan.FromMinutes(Math.Max(0.5, ctx.Minutes * 0.1)), "after-restart");

            return Report(ctx);
        }

        private int Report(StressContext ctx)
        {
            Console.WriteLine($"exact checks: {_checks}, re-reads needed for background writes: {_lagRetries}, collections created during the run: {_newCollections}");
            Console.WriteLine("ops: " + string.Join("  ", _ops.OrderBy(o => o.Key).Select(o => $"{o.Key}={o.Value}")));
            foreach (var e in _errors.OrderByDescending(e => e.Value).Take(8))
                Console.WriteLine($"  {e.Value,6} x {e.Key}");
            return ctx.Finish();
        }

        private async Task RunTraffic(StressContext ctx, DocumentStore store, TimeSpan duration, string phase)
        {
            var database = await GetDatabase(store.Database);
            using var stop = new CancellationTokenSource(duration);

            var workers = Enumerable.Range(0, 4).Select(w => Task.Run(() => Worker(store, database, new Random(ctx.Seed * 31 + w + phase.Length), stop.Token))).ToArray();

            var lastReport = DateTime.UtcNow;
            while (stop.IsCancellationRequested == false && ctx.Failures.Count == 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(2));

                // stop the world: new operations wait, in-flight ones finish, then the committed state is frozen
                _gate.EnterWriteLock();
                try
                {
                    if (Check(ctx, database, phase) == false)
                        break;
                }
                finally
                {
                    _gate.ExitWriteLock();
                }

                if (DateTime.UtcNow - lastReport > TimeSpan.FromSeconds(30))
                {
                    lastReport = DateTime.UtcNow;
                    Console.WriteLine($"  [{phase}] {_checks} exact checks, {_ops.Values.Sum()} ops, {_collections.Count} collections");
                }
            }

            stop.Cancel();
            await Task.WhenAll(workers);
        }

        private void Worker(DocumentStore store, DocumentDatabase database, Random rng, CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                _gate.EnterReadLock();
                try
                {
                    var op = rng.Next(100);
                    var name = op switch
                    {
                        < 40 => Put(store, rng),
                        < 55 => Delete(store, rng),
                        < 62 => BulkInsert(store, rng),
                        < 63 => NewCollections(store, rng),   // 1%: a few hundred new collections over a run, not thousands
                        < 68 => PatchCollection(store, rng),
                        < 70 => EmptyCollection(store, rng),
                        < 72 => CleanupTombstones(database),
                        _ => Touch(store, rng)
                    };
                    _ops.AddOrUpdate(name, 1, (_, c) => c + 1);
                }
                catch (Exception e)
                {
                    var root = e;
                    while (root.InnerException != null)
                        root = root.InnerException;
                    var line = root.Message.Split('\n')[0];
                    _errors.AddOrUpdate($"{root.GetType().Name}: {(line.Length > 90 ? line[..90] : line)}", 1, (_, c) => c + 1);
                }
                finally
                {
                    _gate.ExitReadLock();
                }
            }
        }

        private string PickCollection(Random rng)
        {
            lock (_collectionsLock)
                return _collections[rng.Next(_collections.Count)];
        }

        private string MintCollection()
        {
            var name = $"N{Interlocked.Increment(ref _newCollections):D4}";
            lock (_collectionsLock)
                _collections.Add(name);
            return name;
        }

        private static void Store(Raven.Client.Documents.Session.IDocumentSession session, string collection, string id, Random rng)
        {
            var doc = new Doc { Name = "n" + rng.Next(1000), Payload = new string('p', rng.Next(500)) };
            session.Store(doc, id);
            session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = collection;
        }

        private string Put(DocumentStore store, Random rng)
        {
            var collection = PickCollection(rng);
            using var session = store.OpenSession();
            Store(session, collection, $"{collection.ToLowerInvariant()}/{rng.Next(300)}", rng);
            session.SaveChanges();
            return "put";
        }

        private string Delete(DocumentStore store, Random rng)
        {
            var collection = PickCollection(rng);
            using var session = store.OpenSession();
            session.Delete($"{collection.ToLowerInvariant()}/{rng.Next(300)}");
            session.SaveChanges();
            return "delete";
        }

        private string Touch(DocumentStore store, Random rng)
        {
            var collection = PickCollection(rng);
            var id = $"{collection.ToLowerInvariant()}/{rng.Next(300)}";
            using var session = store.OpenSession();
            var doc = session.Load<Doc>(id);
            if (doc == null)
                return "touch-miss";
            doc.Touched++;
            session.SaveChanges();
            return "touch";
        }

        private string BulkInsert(DocumentStore store, Random rng)
        {
            var collection = PickCollection(rng);
            using var bulk = store.BulkInsert();
            for (var i = 0; i < 20; i++)
            {
                var doc = new Doc { Name = "b" + i, Payload = new string('b', rng.Next(300)) };
                bulk.Store(doc, $"{collection.ToLowerInvariant()}/{rng.Next(300)}", new MetadataAsDictionary { [Constants.Documents.Metadata.Collection] = collection });
            }

            return "bulk";
        }

        // a collection created and written in the same transaction, sometimes two of them, sometimes next to a write
        // into an existing collection - the Index -1 window and the count-based detection at commit
        private string NewCollections(DocumentStore store, Random rng)
        {
            using var session = store.OpenSession();
            var count = 1 + rng.Next(2);
            for (var c = 0; c < count; c++)
            {
                var collection = MintCollection();
                for (var i = 0; i < 1 + rng.Next(3); i++)
                    Store(session, collection, $"{collection.ToLowerInvariant()}/{i}", rng);
            }

            if (rng.Next(2) == 0)
            {
                var existing = PickCollection(rng);
                Store(session, existing, $"{existing.ToLowerInvariant()}/{rng.Next(300)}", rng);
            }

            session.SaveChanges();
            return count == 1 ? "new-collection" : "new-collections-x2";
        }

        private string PatchCollection(DocumentStore store, Random rng)
        {
            var collection = PickCollection(rng);
            var op = store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = $"from '{collection}' update {{ this.Touched = (this.Touched || 0) + 1; }}" }));
            op.WaitForCompletion(TimeSpan.FromMinutes(1));
            return "patch-by-query";
        }

        // empties the collection: its last document etag goes back to 0 while the collection stays in the table
        private string EmptyCollection(DocumentStore store, Random rng)
        {
            var collection = PickCollection(rng);
            var op = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = $"from '{collection}'" }));
            op.WaitForCompletion(TimeSpan.FromMinutes(1));
            return "empty-collection";
        }

        private string CleanupTombstones(DocumentDatabase database)
        {
            database.TombstoneCleaner.ExecuteCleanup().GetAwaiter().GetResult();
            return "tombstone-cleanup";
        }

        private async Task FinalTransaction(DocumentStore store)
        {
            var rng = new Random(12345);
            var victim = _collections.First();

            using var session = store.OpenSession();
            var victimDocs = session.Advanced.LoadStartingWith<Doc>(victim.ToLowerInvariant() + "/", pageSize: 1024);
            foreach (var doc in victimDocs)
                session.Delete(doc);

            for (var c = 0; c < 2; c++)
            {
                var collection = MintCollection();
                Store(session, collection, $"{collection.ToLowerInvariant()}/0", rng);
            }

            session.SaveChanges();
            Console.WriteLine($"final transaction: emptied '{victim}' ({victimDocs.Length} documents) and created two collections in the same transaction");
            await Task.CompletedTask;
        }

        // (collection -> "doc etag / tombstone etag") as a read transaction serves it
        private static Dictionary<string, string> Served(DocumentDatabase database)
        {
            var storage = database.DocumentsStorage;
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
                return Values(storage, context, context.Transaction);
        }

        private static Dictionary<string, string> Values(DocumentsStorage storage, DocumentsOperationContext context, DocumentsTransaction tx)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var collection in storage.GetCollections(context).Select(c => c.Name))
                result[collection] = $"doc {storage.GetLastDocumentEtag(tx.InnerTransaction, collection)} / tombstone {storage.GetLastTombstoneEtag(tx.InnerTransaction, collection)}";
            return result;
        }

        private bool Check(StressContext ctx, DocumentDatabase database, string phase)
        {
            var storage = database.DocumentsStorage;
            Interlocked.Increment(ref _checks);

            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeContext))
            using (writeContext.OpenWriteTransaction())
            {
                // holding the write transaction freezes the committed state; a write transaction never uses the cache
                var truth = Values(storage, writeContext, writeContext.Transaction);

                string mismatch = null;
                for (var attempt = 0; attempt < 25; attempt++)
                {
                    using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readContext))
                    using (readContext.OpenReadTransaction())
                        mismatch = Diff(truth, Values(storage, readContext, readContext.Transaction));

                    if (mismatch == null)
                        return true;

                    // a background write committed just before we took the lock may not be visible to readers yet
                    Interlocked.Increment(ref _lagRetries);
                    Thread.Sleep(20);
                }

                ctx.Fail($"[{phase}] the cache served by a read transaction disagrees with storage and did not converge within 500ms while the write lock was held: {mismatch}");
                return false;
            }
        }

        private static string Diff(Dictionary<string, string> truth, Dictionary<string, string> served)
        {
            foreach (var (collection, value) in truth)
            {
                if (served.TryGetValue(collection, out var s) == false)
                    return $"collection '{collection}' exists in storage ({value}) but the cache does not list it";
                if (s != value)
                    return $"collection '{collection}': storage {value}, cache {s}";
            }

            var extra = served.Keys.Except(truth.Keys).FirstOrDefault();
            return extra == null ? null : $"collection '{extra}' is listed by the cache ({served[extra]}) but not by storage";
        }
    }
}
