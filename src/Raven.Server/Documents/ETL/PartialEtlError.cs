using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class PartialEtlError
{
    public string Id => GetId();
    public string EtlTaskName { get; set; }
    public DateTime CreatedAt { get; set; }
    public string DocumentId { get; set; }
    public EtlErrorStep Step { get; set; }
    public EtlErrorSeverity Severity { get; set; }
    
    private string GetId()
    {
        return $"{EtlTaskName}/{DocumentId}";
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(EtlTaskName)] = EtlTaskName,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(DocumentId)] = DocumentId,
            [nameof(Step)] = Step,
            [nameof(Severity)] = Severity,
        };
    }
}
