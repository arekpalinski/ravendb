﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class ShardedBatchCommandsReader : AbstractBatchCommandsReader<ShardedBatchCommand, TransactionOperationContext>
{
    public List<Stream> Streams;
    public List<BufferedCommand> BufferedCommands = new();

    private readonly ShardedDatabaseContext _databaseContext;

    public ShardedBatchCommandsReader(ShardedDatabaseRequestHandler handler) :
            base(handler, handler.DatabaseContext.DatabaseName, handler.DatabaseContext.IdentityPartsSeparator, BatchRequestParser.Instance)
    {
        _databaseContext = handler.DatabaseContext;
    }

    public override async Task SaveStream(JsonOperationContext context, Stream input)
    {
        Streams ??= new List<Stream>();
        var attachment = ServerStore.GetTempFile($"{_databaseContext.DatabaseName}.attachment", "sharded").StartNewStream();
        await input.CopyToAsync(attachment, Handler.AbortRequestToken);
        await attachment.FlushAsync(Handler.AbortRequestToken);
        Streams.Add(attachment);
    }

    public override async Task<BatchRequestParser.CommandData> ReadCommand(
        JsonOperationContext ctx,
        Stream stream, JsonParserState state,
        UnmanagedJsonParser parser,
        JsonOperationContext.MemoryBuffer buffer,
        BlittableMetadataModifier modifier,
        CancellationToken token)
    {
        var ms = new MemoryStream();
        try
        {
            var bufferedCommand = new BufferedCommand { CommandStream = ms };
            var result = await BatchRequestParser.Instance.ReadAndCopySingleCommand(ctx, stream, state, parser, buffer, bufferedCommand, modifier, token);
            bufferedCommand.IsIdentity = IsIdentityCommand(ref result);
            BufferedCommands.Add(bufferedCommand);
            return result;
        }
        catch
        {
            await ms.DisposeAsync();
            throw;
        }
    }

    public override async ValueTask<ShardedBatchCommand> GetCommandAsync(TransactionOperationContext context)
    {
        await ExecuteGetIdentitiesAsync();

        return new ShardedBatchCommand(context, _databaseContext)
        {
            ParsedCommands = Commands,
            BufferedCommands = BufferedCommands,
            AttachmentStreams = Streams,
            IsClusterTransaction = IsClusterTransactionRequest
        };
    }
}
