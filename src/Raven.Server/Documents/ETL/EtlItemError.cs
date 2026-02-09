using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class EtlItemError : EtlErrorBase
{
    public string DocumentId { get; set; }

    protected override string GetId()
    {
        return $"{EtlProcessName}/{DocumentId}";
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(DocumentId)] = DocumentId;

        return json;
    }
}
