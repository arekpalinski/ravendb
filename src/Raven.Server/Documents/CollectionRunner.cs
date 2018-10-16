using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents
{
    internal class CollectionRunner
    {
        private readonly IndexQueryServerSide _collectionQuery;

        protected readonly DocumentsOperationContext Context;
        protected readonly DocumentDatabase Database;

        public CollectionRunner(DocumentDatabase database, DocumentsOperationContext context, IndexQueryServerSide collectionQuery)
        {
            Debug.Assert(collectionQuery == null || collectionQuery.Metadata.IsCollectionQuery);

            Database = database;
            Context = context;
            _collectionQuery = collectionQuery;
        }

        public virtual Task<IOperationResult> ExecuteDelete(string collectionName, CollectionOperationOptions options, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, options, Context, onProgress, key => new DeleteDocumentCommand(key, null, Database), token);
        }

        public Task<IOperationResult> ExecutePatch(string collectionName, CollectionOperationOptions options, PatchRequest patch,
            BlittableJsonReaderObject patchArgs, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, options, Context, onProgress,
                key => new PatchDocumentCommand(Context, key, null, false, (patch, patchArgs), (null, null),
                    Database, false, false, false), token);
        }

        public static int Total = 0;

        public static ManualResetEvent WaitForPatchedDocs = new ManualResetEvent(false);

        //public static object Lock { get; set; } = new object();
        //public static QueuedLock Lock { get; set; } = new QueuedLock();

        public sealed class QueuedLock
        {
            private object innerLock;
            private volatile int ticketsCount = 0;
            private volatile int ticketToRide = 1;

            public QueuedLock()
            {
                innerLock = new Object();
            }

            public void Enter()
            {
                int myTicket = Interlocked.Increment(ref ticketsCount);
                Monitor.Enter(innerLock);
                while (true)
                {

                    if (myTicket == ticketToRide)
                    {
                        return;
                    }
                    else
                    {
                        Monitor.Wait(innerLock);
                    }
                }
            }

            public void Exit()
            {
                Interlocked.Increment(ref ticketToRide);
                Monitor.PulseAll(innerLock);
                Monitor.Exit(innerLock);
            }
        }

        protected async Task<IOperationResult> ExecuteOperation(string collectionName, CollectionOperationOptions options, DocumentsOperationContext context,
             Action<DeterminateProgress> onProgress, Func<LazyStringValue, TransactionOperationsMerger.MergedTransactionCommand> action, OperationCancelToken token)
        {
            const int batchSize = 8192;
            var progress = new DeterminateProgress();
            var cancellationToken = token.Token;
            var isAllDocs = collectionName == Constants.Documents.Collections.AllDocumentsCollection;


            long lastEtag;
            long totalCount;
            using (context.OpenReadTransaction())
            {
                lastEtag = GetLastEtagForCollection(context, collectionName, isAllDocs);
                totalCount = GetTotalCountForCollection(context, collectionName, isAllDocs);
            }

            Console.WriteLine("Last etag: " + lastEtag);


            progress.Total = totalCount;

            // send initial progress with total count set, and 0 as processed count
            onProgress(progress);

            long startEtag = 679937;// 401409 /*778241*/;//401409;//350209;//301057;//200705;//104449;
            using (var rateGate = options.MaxOpsPerSecond.HasValue
                    ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1))
                    : null)
            {
                var end = false;

                var total = 0;

                while (startEtag <= lastEtag)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    WaitForPatchedDocs.Reset();

                    //Lock.Enter();

                    

                    try
                    {
                        using (context.OpenReadTransaction())
                        {
                            var ids = new Queue<LazyStringValue>(batchSize);

                            Console.WriteLine("Collection runner, start: " + startEtag);

                            //if (startEtag >= 778241)
                            //{
                            //    Console.WriteLine("STOP: " + startEtag);

                            //    Console.ReadKey();
                            //}

                            //if (total >= 278528)
                            //{
                            //    Console.WriteLine("STOP: it processed  " + total + " start etag: " + startEtag);

                            //    Console.ReadKey();
                            //}

                            foreach (var document in GetDocuments(context, collectionName, startEtag, batchSize, isAllDocs))
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                token.Delay();

                                if (isAllDocs && document.Id.StartsWith(HiLoHandler.RavenHiloIdPrefix, StringComparison.OrdinalIgnoreCase))
                                    continue;

                                if (document.Etag > lastEtag) // we don't want to go over the documents that we have patched
                                {
                                    end = true;
                                    break;
                                }

                                startEtag = document.Etag + 1;

                                ids.Enqueue(document.Id);

                                total++;


                                //if (total >= 380681)
                                //{
                                //    Console.WriteLine("ENDDDDD");

                                //    end = true;
                                //    break;
                                //}
                            }

                            if (ids.Count == 0)
                                break;




                            do
                            {


                                var command = new ExecuteRateLimitedOperations<LazyStringValue>(ids, action, rateGate, token,
                                    maxTransactionSize: 16 * Voron.Global.Constants.Size.Megabyte,
                                    batchSize: batchSize);

                                Database.TxMerger.Enqueue(command).Wait();


                                //Console.WriteLine("Patched: " + ids.Count);

                                progress.Processed += command.Processed;

                                Total += command.Processed;
                                Console.WriteLine("Processed: " + progress.Processed + " total: " + total);

                                onProgress(progress);

                                if (DocumentsStorage.ShouldStop)
                                {
                                    Console.WriteLine("Should Stop " + ids.Count);

                                    end = true;
                                    break;
                                }

                                if (command.NeedWait)
                                    rateGate?.WaitToProceed();

                            } while (ids.Count > 0);

                            if (end)
                                break;
                        }
                    }
                    finally
                    {
                        //if (progress.Processed > 250_000)
                        //{
                        //    Console.WriteLine($"PATCHING has processed {progress.Processed} documents. Please disable indexing .... ");

                        //    Console.ReadLine();

                        //    Console.WriteLine("Continue to work");
                        //}

                        WaitForPatchedDocs.Set();
                        Index.IndexingCompleted.Reset();

                        //Lock.Exit();
                    }

                    Index.IndexingCompleted.WaitOne();

                    await Task.Delay(TimeSpan.FromSeconds(2));
                }
            }

            return new BulkOperationResult
            {
                Total = progress.Processed
            };
        }

        protected virtual IEnumerable<Document> GetDocuments(DocumentsOperationContext context, string collectionName, long startEtag, int batchSize, bool isAllDocs)
        {
            if (_collectionQuery != null && _collectionQuery.Metadata.WhereFields.Count > 0)
            {
                return new CollectionQueryEnumerable(Database, Database.DocumentsStorage, new FieldsToFetch(_collectionQuery, null),
                    collectionName, _collectionQuery, null, context, null, new Reference<int>());
            }

            if (isAllDocs)
                return Database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, batchSize);

            return Database.DocumentsStorage.GetDocumentsFrom(context, collectionName, startEtag, 0, batchSize);
        }

        protected virtual long GetTotalCountForCollection(DocumentsOperationContext context, string collectionName, bool isAllDocs)
        {
            if (isAllDocs)
            {
                var allDocsCount = Database.DocumentsStorage.GetNumberOfDocuments(context);
                Database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, CollectionName.HiLoCollection, 0, out long hiloDocsCount);
                return allDocsCount - hiloDocsCount;
            }

            Database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, collectionName, 0, out long totalCount);
            return totalCount;
        }

        protected virtual long GetLastEtagForCollection(DocumentsOperationContext context, string collection, bool isAllDocs)
        {
            if (isAllDocs)
                return DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);

            return Database.DocumentsStorage.GetLastDocumentEtag(context, collection);
        }
    }
}
