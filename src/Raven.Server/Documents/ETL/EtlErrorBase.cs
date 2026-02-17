using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public abstract class EtlErrorBase
{
    public string Id => GetId();
    public string EtlProcessName { get; set; }
    public DateTime CreatedAt { get; set; }
    public EtlErrorStep Step { get; set; }
    public string Error { get; set; }
    public string AdditionalInfo { get; set; } = string.Empty;

    protected virtual string GetId() => throw new NotImplementedException();

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(EtlProcessName)] = EtlProcessName,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(Step)] = Step,
            [nameof(Error)] = Error,
            [nameof(AdditionalInfo)] = AdditionalInfo
        };
    }
}
