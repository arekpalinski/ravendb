using System.IO;
using System.Threading;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedBulkInsertCommandsBuilder : BatchRequestParser.ReadMany
{
    public ShardedBulkInsertCommandsBuilder(JsonOperationContext ctx, Stream stream, JsonOperationContext.MemoryBuffer buffer, BatchRequestParser batchRequestParser, CancellationToken token) : base(ctx, stream, buffer, batchRequestParser, token)
    {
    }
}
