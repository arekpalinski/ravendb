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

internal sealed class EtlHandlerProcessorForGetErrors : AbstractEtlHandlerProcessorForGetErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    protected override bool SupportsCurrentNode => true;
    
    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var response = new Response();

        var storage = RequestHandler.Database.EtlErrorsStorage;
        var processNames = GetNames().ToList();
        
        if (processNames.Count == 0)
            processNames = RequestHandler.Database.EtlLoader.Processes.Select(x => x.Name).ToList();
        
        foreach (var etlProcessName in processNames)
        {
            using (storage.ReadProcessErrorsOfEtl(etlProcessName, out var processErrors))
            using (storage.ReadItemErrorsOfEtl(etlProcessName, out var itemErrors))
            {
                var etlProcessErrors = new EtlErrors()
                {
                    ProcessName = etlProcessName,
                    ProcessErrors = processErrors.Select(x => x.ToEtlProcessError()).ToArray(),
                    ItemErrors = itemErrors.Select(x => x.ToEtlItemError()).ToArray()
                };
                
                response.Results.Add(etlProcessErrors);
            }
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, nameof(Response.Results), response.Results, (w, c, errors) => w.WriteObject(c.ReadObject(errors.ToJson(), "etl/errors")));
                writer.WriteEndObject();
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlErrors[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    internal class Response
    {
        public List<EtlErrors> Results { get; set; } = new List<EtlErrors>();        
    }
}

