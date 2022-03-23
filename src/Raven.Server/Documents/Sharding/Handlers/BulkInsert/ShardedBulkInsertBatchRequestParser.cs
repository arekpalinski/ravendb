using System.IO;
using System.Threading;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedBulkInsertBatchRequestParser : BatchRequestParser.ReadMany
{
    public ShardedBulkInsertBatchRequestParser(JsonOperationContext ctx, Stream stream, JsonOperationContext.MemoryBuffer buffer, CancellationToken token) : base(ctx, stream, buffer, token)
    {
    }
}
