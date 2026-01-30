using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class GetEtlErrorsCommand : RavenCommand<EtlProcessErrors[]>
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
            for (var i = 0; i < _names.Length; i++)
                url += $"{(i == 0 ? "?" : "&")}name={Uri.EscapeDataString(_names[i])}";
        }

        var request = new HttpRequestMessage { Method = HttpMethod.Get };

        return request;
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
        public EtlProcessErrors[] Results { get; set; }
    }
}
