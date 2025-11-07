using System;

namespace Raven.Server.Documents.ETL;

public class EtlErrorTableValue
{
    public DateTime CreatedAt;
    public long AffectedDocumentsCount;
    public long Step;
    public long Severity;

    public EtlError ToEtlError()
    {
        return new EtlError
        {
            CreatedAt = CreatedAt,
            AffectedDocumentsCount = AffectedDocumentsCount,
            Step = (EtlErrorStep)Step,
            Severity = (EtlErrorSeverity)Severity
        };
    }
}
