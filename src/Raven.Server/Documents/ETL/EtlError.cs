using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class EtlError
{
    public string Id => GetId();
    // todo update this on task/transformation rename
    public string EtlTaskName { get; set; }
    public DateTime CreatedAt { get; set; }
    public long AffectedDocumentsCount { get; set; }
    public EtlErrorStep Step { get; set; }
    public EtlErrorSeverity Severity { get; set; }
    public string Message { get; set; }
    
    private string GetId()
    {
        return $"{EtlTaskName}/{CreatedAt.Ticks}";
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(EtlTaskName)] = EtlTaskName,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(AffectedDocumentsCount)] = AffectedDocumentsCount,
            [nameof(Step)] = Step,
            [nameof(Severity)] = Severity,
            [nameof(Message)] = Message
        };
    }
}
