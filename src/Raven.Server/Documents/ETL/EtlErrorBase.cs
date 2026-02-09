using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public abstract class EtlErrorBase
{
    public string Id => GetId();
    // todo update this on task/transformation rename
    public string EtlProcessName { get; set; }
    public DateTime CreatedAt { get; set; }
    public EtlErrorStep Step { get; set; }
    public EtlErrorSeverity Severity { get; set; }
    public string Error { get; set; }

    protected virtual string GetId() => throw new NotImplementedException();

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(EtlProcessName)] = EtlProcessName,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(Step)] = Step,
            [nameof(Severity)] = Severity,
            [nameof(Error)] = Error
        };
    }
}
