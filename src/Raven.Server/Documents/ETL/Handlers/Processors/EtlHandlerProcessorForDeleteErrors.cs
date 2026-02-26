using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForDeleteErrors : AbstractEtlHandlerProcessorForDeleteErrors<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForDeleteErrors(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;
    
    protected override ValueTask HandleCurrentNodeAsync()
    {
        var etlProcessName = GetEtlProcessName();
        
        if (etlProcessName != null)
            RequestHandler.Database.EtlErrorsStorage.DeleteErrorsOfEtl(etlProcessName);
        else
            RequestHandler.Database.EtlErrorsStorage.DeleteAllEtlErrors();
        
        return ValueTask.CompletedTask;
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
