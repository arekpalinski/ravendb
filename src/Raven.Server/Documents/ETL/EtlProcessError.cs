using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class EtlProcessError : EtlErrorBase
{
    public long AffectedDocumentsCount { get; set; }
    
    protected override string GetId()
    {
        return $"{EtlProcessName}/{CreatedAt.Ticks}";
    }

    public override DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(EtlProcessName)] = EtlProcessName,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(AffectedDocumentsCount)] = AffectedDocumentsCount,
            [nameof(Step)] = Step,
            [nameof(Severity)] = Severity,
            [nameof(Error)] = Error
        };
    }
}
