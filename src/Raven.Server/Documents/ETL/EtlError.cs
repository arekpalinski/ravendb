using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class EtlError
{
    public string Id => GetId();
    public DateTime CreatedAt { get; set; }
    public long AffectedDocumentsCount { get; set; }
    public EtlErrorStep Step { get; set; }
    public EtlErrorSeverity Severity { get; set; }
    
    private string GetId()
    {
        return $"{Step}";
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(AffectedDocumentsCount)] = AffectedDocumentsCount,
            [nameof(Step)] = Step.ToString(),
            [nameof(Severity)] = Severity.ToString(),
        };
    }
}
