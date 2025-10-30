using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractEtlHandlerProcessorForErrors<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<EtlTaskErrors[], TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractEtlHandlerProcessorForErrors([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
        
    }
    
    protected override RavenCommand<EtlTaskErrors[]> CreateCommandForNode(string nodeTag)
    {
        var names = GetNames();

        return new GetEtlErrorsCommand(names, nodeTag);
    }

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
}
