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
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(EtlProcessName)] = EtlProcessName,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(DocumentId)] = DocumentId,
            [nameof(Step)] = Step,
            [nameof(Severity)] = Severity,
        };
    }
}
