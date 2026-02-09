using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL;

public class EtlProcessError : EtlErrorBase
{
    public long AffectedDocumentsCount { get; set; }
    public DateTime? NextBatchRetryTime { get; set; }
    
    protected override string GetId()
    {
        return $"{EtlProcessName}/{CreatedAt.Ticks}";
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(AffectedDocumentsCount)] = AffectedDocumentsCount;
        json[nameof(NextBatchRetryTime)] = NextBatchRetryTime;

        return json;
    }
}
