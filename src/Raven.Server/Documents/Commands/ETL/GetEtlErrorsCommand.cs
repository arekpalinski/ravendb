using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class GetEtlErrorsCommand : RavenCommand<EtlErrors[]>
{
    private readonly string[] _names;

    public GetEtlErrorsCommand(string[] names, string nodeTag)
    {
        _names = names;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/etl/errors";

        if (_names is { Length: > 0 })
        {
            foreach (var name in _names)
            {
                url = QueryHelpers.AddQueryString(url, "name", name);
            }
        }

        return new HttpRequestMessage { Method = HttpMethod.Get };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<EtlTaskErrorsResponse>(response).Results;
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    private sealed class EtlTaskErrorsResponse
    {
        public EtlErrors[] Results { get; set; }
    }
}
