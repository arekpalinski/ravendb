using System;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL;

public class EtlErrorTableValue
{
    public DateTime CreatedAt;
    public string EtlTaskName;
    public long AffectedDocumentsCount;
    public long Step;
    public long Severity;

    public EtlError ToEtlError()
    {
        return new EtlError
        {
            CreatedAt = CreatedAt,
            EtlTaskName = EtlTaskName,
            AffectedDocumentsCount = AffectedDocumentsCount,
            Step = (EtlErrorStep)Step,
            Severity = (EtlErrorSeverity)Severity
        };
    }
}
