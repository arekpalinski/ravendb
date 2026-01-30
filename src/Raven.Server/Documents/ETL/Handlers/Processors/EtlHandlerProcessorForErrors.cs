using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
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
            foreach (var process in etlKvp.Value)
            {
                var processName = process.Name;
                
                using (storage.ReadProcessErrorsOfTask(processName, out var processErrors))
                using (storage.ReadItemErrorsOfTask(processName, out var itemErrors))
                {
                    var etlProcessErrors = new EtlProcessErrors()
                    {
                        ProcessName = processName,
                        ProcessErrors = processErrors.Select(x => x.ToEtlProcessError()).ToArray(),
                        ItemErrors = itemErrors.Select(x => x.ToEtlItemError()).ToArray()
                    };
                
                    response.Results.Add(etlProcessErrors);
                }
            }
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, nameof(Response.Results), response.Results, (w, c, stats) => w.WriteObject(c.ReadObject(stats.ToJson(), "etl/errors")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlProcessErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    internal class Response
    {
        public List<EtlProcessErrors> Results { get; set; } = new List<EtlProcessErrors>();        
    }
}

