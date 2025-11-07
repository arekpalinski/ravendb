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
        
        var tasksErrorsList = new List<EtlTaskErrors>();

        foreach (var etlKvp in etlsToReportOn)
        {
            var taskName = etlKvp.Key;
            using (storage.ReadErrorsOrderedByCreationDate(out var errors))
            using (storage.ReadPartialErrorsOrderedByCreationDate(out var partialErrors))
            {
                var taskErrors = new EtlTaskErrors()
                {
                    TaskName = taskName,
                    Errors = errors.Select(x => x.ToEtlError()).ToArray(),
                    PartialErrors = partialErrors.Select(x => x.ToPartialEtlError()).ToArray()
                };
                
                tasksErrorsList.Add(taskErrors);
            };
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", tasksErrorsList, (w, c, stats) => w.WriteObject(c.ReadObject(stats.ToJson(), "etl/errors")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}

