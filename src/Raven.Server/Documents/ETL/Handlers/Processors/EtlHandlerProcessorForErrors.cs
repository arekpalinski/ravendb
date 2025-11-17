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
        
        var etlsToReportOn = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names);
        var storage = RequestHandler.Database.EtlErrorsStorage;
        
        var response = new Response();

        foreach (var etlKvp in etlsToReportOn)
        {
            // todo fix
            var taskName = etlKvp.Value.First().Name;
            using (storage.ReadErrorsOfTask(taskName, out var errors))
            using (storage.ReadPartialErrorsOfTask(taskName, out var partialErrors))
            {
                var taskErrors = new EtlTaskErrors()
                {
                    TaskName = taskName,
                    Errors = errors.Select(x => x.ToEtlError()).ToArray(),
                    PartialErrors = partialErrors.Select(x => x.ToPartialEtlError()).ToArray()
                };
                
                response.Results.Add(taskErrors);
            }
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", response.Results, (w, c, stats) => w.WriteObject(c.ReadObject(stats.ToJson(), "etl/errors")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    internal class Response
    {
        public List<EtlTaskErrors> Results { get; set; } = new List<EtlTaskErrors>();        
    }
}

