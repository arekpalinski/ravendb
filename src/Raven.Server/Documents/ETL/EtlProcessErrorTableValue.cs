using System;

namespace Raven.Server.Documents.ETL;

public class EtlProcessErrorTableValue : EtlErrorTableValueBase
{
    public long AffectedDocumentsCount;
    public DateTime? NextBatchRetryTime;
    
    protected override string GetId()
    {
        return $"{EtlProcessName}/{CreatedAt.Ticks}";
    }

    public EtlProcessError ToEtlProcessError()
    {
        return new EtlProcessError
        {
            CreatedAt = CreatedAt,
            EtlProcessName = EtlProcessName,
            AffectedDocumentsCount = AffectedDocumentsCount,
            Step = (EtlErrorStep)Step,
            Severity = (EtlErrorSeverity)Severity,
            Error = Error,
            NextBatchRetryTime = NextBatchRetryTime
        };
    }
}
