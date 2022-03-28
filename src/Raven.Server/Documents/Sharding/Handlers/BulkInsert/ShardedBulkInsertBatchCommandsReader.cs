using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Batching;
using Raven.Server.Documents.Sharding.Handlers.Batches;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedBulkInsertBatchCommandsReader : AbstractBatchCommandsReader<ShardedBatchCommand, TransactionOperationContext>
{
    private readonly CommandBufferingBatchRequestParser _batchRequestParser;
    public List<Stream> Streams;

    private readonly bool _encrypted;
    private readonly ShardedDatabaseContext _databaseContext;

    public ShardedBulkInsertBatchCommandsReader(ShardedRequestHandler handler, CommandBufferingBatchRequestParser batchRequestParser) :
        base(handler, handler.ShardedContext.DatabaseName, handler.ShardedContext.IdentitySeparator, batchRequestParser)
    {
        _batchRequestParser = batchRequestParser;
        _databaseContext = handler.ShardedContext;
        _encrypted = handler.ShardedContext.Encrypted;
    }

    public async Task<ShardedCommandData> ReadShardedCommand(
        JsonOperationContext ctx,
        Stream stream, JsonParserState state,
        UnmanagedJsonParser parser,
        JsonOperationContext.MemoryBuffer buffer,
        BlittableMetadataModifier modifier,
        CancellationToken token)
    {
        var command = await base.ReadCommand(ctx, stream, state, parser, buffer, modifier, token);

        var result = new ShardedCommandData(command, ctx);

        await _batchRequestParser.CopyLastCommandStreamToAsync(result.Stream);

        return result;
    }

    public override async Task SaveStream(JsonOperationContext context, Stream input)
    {
        Streams ??= new List<Stream>();
        var attachment = GetServerTempFile("sharded").StartNewStream();
        await input.CopyToAsync(attachment, Handler.AbortRequestToken);
        await attachment.FlushAsync(Handler.AbortRequestToken);
        Streams.Add(attachment);
    }

    public override ValueTask<ShardedBatchCommand> GetCommandAsync(TransactionOperationContext context)
    {
        throw new NotImplementedException();
    }

    public StreamsTempFile GetServerTempFile(string prefix)
    {
        var name = $"{_shardedContext.DatabaseName}.attachment.{Guid.NewGuid():N}.{prefix}";
        var tempPath = ServerStore._env.Options.DataPager.Options.TempPath.Combine(name);

        return new StreamsTempFile(tempPath.FullPath, _encrypted);
    }
}
