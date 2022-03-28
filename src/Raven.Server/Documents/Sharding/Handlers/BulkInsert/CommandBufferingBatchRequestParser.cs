using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class CommandBufferingBatchRequestParser : BatchRequestParser, IDisposable
{
    private readonly JsonOperationContext _ctx;
    private readonly MemoryStream _commandStream;

    public CommandBufferingBatchRequestParser(JsonOperationContext ctx)
    {
        _ctx = ctx;
        _commandStream = ctx.CheckoutMemoryStream(); // TODO arek - let's use memory stream for now, then use context.GetMemoryBuffer(out var buffer)
    }

    public Task CopyLastCommandStreamToAsync(MemoryStream stream)
    {
        return _commandStream.CopyToAsync(stream);
    }

    public void Dispose()
    {
        _ctx.ReturnMemoryStream(_commandStream);
    }
}
