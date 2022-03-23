using System;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding.Operations.BulkInsert;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedBulkInsertHandler : ShardedRequestHandler
{
    [RavenShardedAction("/databases/*/bulk_insert", "POST")]
    public async Task BulkInsert()
    {
        var id = GetLongQueryString("id");
        var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;
    }

    /*private async Task<IOperationResult> DoBulkInsert(Action<IOperationProgress> onProgress, long id, bool skipOverwriteIfUnchanged, CancellationToken token)
    {
        var progress = new BulkInsertProgress();
        try
        {
            var logger = LoggingSource.Instance.GetLogger<BulkInsertHandler.MergedInsertBulkCommand>(ShardedContext.DatabaseName);
            IDisposable currentCtxReset = null, previousCtxReset = null;

            try
            {


                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.GetMemoryBuffer(out var buffer))
                await using (var operation = new ShardedBulkInsertOperation(id, skipOverwriteIfUnchanged, ShardedContext))
                {
                    var requestBodyStream = RequestBodyStream();

                    // ClientAcceptsGzipResponse() == false ? CompressionLevel.NoCompression : CompressionLevel.Optimal // TODO arek
                    if (requestBodyStream is GZipStream)
                    {
                        operation.CompressionLevel = CompressionLevel.Optimal; // TODO arek
                    }

                    currentCtxReset = ContextPool.AllocateOperationContext(out JsonOperationContext docsCtx);


                    using (var parser = new BatchRequestParser.ReadMany(context, requestBodyStream, buffer, token))
                    {
                        await parser.Init();

                        var array = new BatchRequestParser.CommandData[8];
                        var numberOfCommands = 0;
                        long totalSize = 0;
                        int operationsCount = 0;

                        while (true)
                        {
                            using (var modifier = new BlittableMetadataModifier(docsCtx))
                            {
                                var task = parser.MoveNext(docsCtx, modifier);
                                if (task == null)
                                    break;

                                token.ThrowIfCancellationRequested();

                                // if we are going to wait on the network, flush immediately
                                if ((task.Wait(5) == false && numberOfCommands > 0) ||
                                    // but don't batch too much anyway
                                    totalSize > 16 * Voron.Global.Constants.Size.Megabyte || operationsCount >= 8192)
                                {
                                    using (ReplaceContextIfCurrentlyInUse(task, numberOfCommands, array))
                                    {
                                        foreach (BatchRequestParser.CommandData data in array)
                                        {
                                            int shardNumber = ShardedContext.GetShardNumber(context, data.Id);

                                            operation.StoreAsync()

                                        }

                                        // TODO arek
                                        //await Database.TxMerger.Enqueue(new BulkInsertHandler.MergedInsertBulkCommand
                                        //{
                                        //    Commands = array,
                                        //    NumberOfCommands = numberOfCommands,
                                        //    Database = Database,
                                        //    Logger = logger,
                                        //    TotalSize = totalSize,
                                        //    SkipOverwriteIfUnchanged = skipOverwriteIfUnchanged
                                        //});
                                    }

                                    ClearStreamsTempFiles();

                                    progress.BatchCount++;
                                    progress.Total += numberOfCommands;
                                    progress.LastProcessedId = array[numberOfCommands - 1].Id;

                                    onProgress(progress);

                                    previousCtxReset?.Dispose();
                                    previousCtxReset = currentCtxReset;
                                    currentCtxReset = ContextPool.AllocateOperationContext(out docsCtx);

                                    numberOfCommands = 0;
                                    totalSize = 0;
                                    operationsCount = 0;
                                }

                                var commandData = await task;
                                if (commandData.Type == CommandType.None)
                                    break;

                                if (commandData.Type == CommandType.AttachmentPUT)
                                {
                                    commandData.AttachmentStream = await WriteAttachment(commandData.ContentLength, parser.GetBlob(commandData.ContentLength));
                                }

                                (long size, int opsCount) = GetSizeAndOperationsCount(commandData);
                                operationsCount += opsCount;
                                totalSize += size;
                                if (numberOfCommands >= array.Length)
                                    Array.Resize(ref array, array.Length + Math.Min(1024, array.Length));
                                array[numberOfCommands++] = commandData;

                                switch (commandData.Type)
                                {
                                    case CommandType.PUT:
                                        progress.DocumentsProcessed++;
                                        break;

                                    case CommandType.AttachmentPUT:
                                        progress.AttachmentsProcessed++;
                                        break;

                                    case CommandType.Counters:
                                        progress.CountersProcessed++;
                                        break;

                                    case CommandType.TimeSeriesBulkInsert:
                                        progress.TimeSeriesProcessed++;
                                        break;
                                }
                            }
                        }

                        if (numberOfCommands > 0)
                        {
                            await Database.TxMerger.Enqueue(new BulkInsertHandler.MergedInsertBulkCommand
                            {
                                Commands = array,
                                NumberOfCommands = numberOfCommands,
                                Database = Database,
                                Logger = logger,
                                TotalSize = totalSize,
                                SkipOverwriteIfUnchanged = skipOverwriteIfUnchanged
                            });

                            progress.BatchCount++;
                            progress.Total += numberOfCommands;
                            progress.LastProcessedId = array[numberOfCommands - 1].Id;
#pragma warning disable CS0618 // Type or member is obsolete
                            progress.Processed = progress.DocumentsProcessed;
#pragma warning restore CS0618 // Type or member is obsolete

                            onProgress(progress);
                        }
                    }
                }
            }
            finally
            {
                currentCtxReset?.Dispose();
                previousCtxReset?.Dispose();
                ClearStreamsTempFiles();
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            return new BulkOperationResult
            {
                Total = progress.Total,
                DocumentsProcessed = progress.DocumentsProcessed,
                AttachmentsProcessed = progress.AttachmentsProcessed,
                CountersProcessed = progress.CountersProcessed,
                TimeSeriesProcessed = progress.TimeSeriesProcessed
            };
        }
        catch (Exception e)
        {
            HttpContext.Response.Headers["Connection"] = "close";

            throw new InvalidOperationException("Failed to process bulk insert. " + progress, e);
        }
    }

    private IDisposable ReplaceContextIfCurrentlyInUse(Task<BatchRequestParser.CommandData> task, int numberOfCommands, BatchRequestParser.CommandData[] array)
    {
        if (task.IsCompleted)
            return null;

        var disposable = ContextPool.AllocateOperationContext(out JsonOperationContext tempCtx);
        // the docsCtx is currently in use, so we
        // cannot pass it to the tx merger, we'll just
        // copy the documents to a temporary ctx and
        // use that ctx instead. Copying the documents
        // is safe, because they are immutables

        for (int i = 0; i < numberOfCommands; i++)
        {
            if (array[i].Document != null)
            {
                array[i].Document = array[i].Document.Clone(tempCtx);
            }
        }
        return disposable;
    }*/
}
