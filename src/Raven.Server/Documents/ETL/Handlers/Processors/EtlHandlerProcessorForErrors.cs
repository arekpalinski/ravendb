using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForErrors : AbstractEtlHandlerProcessorForErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    protected override bool SupportsCurrentNode => true;
    
    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var names = GetNames();
        var debugStats = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names)
            .Select(x => new EtlTaskErrors
            {
                TaskName = x.Key,
                Errors = x.Value.Select(y => new EtlProcessTransformationDebugStats
                {
                    TransformationName = y.TransformationName,
                    Statistics = y.Statistics,
                    Metrics = y.Metrics
                }).ToArray()
            }).ToArray();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", debugStats, (w, c, stats) => w.WriteObject(c.ReadObject(stats.ToJson(), "etl/errors")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}

