using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class DeleteEtlErrorsCommand : RavenCommand
{
    private readonly string _etlProcessName;
    
    public DeleteEtlErrorsCommand(string nodeTag, string etlProcessName)
    {
        _etlProcessName = etlProcessName;
        SelectedNodeTag = nodeTag;
    }
    
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var baseUrl = $"{node.Url}/databases/{node.Database}/etl/errors";
        
        url = QueryHelpers.AddQueryString(baseUrl, "name", _etlProcessName);

        return new HttpRequestMessage { Method = HttpMethod.Delete };
    }
}
