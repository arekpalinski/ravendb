using System;

namespace Raven.Server.Documents.ETL;

public class PartialEtlErrorTableValue
{
    public DateTime CreatedAt;
    public string DocumentId;
    public long Step;
    public long Severity;
    
    public PartialEtlError ToPartialEtlError()
    {
        return new PartialEtlError
        {
            CreatedAt = CreatedAt,
            DocumentId = DocumentId,
            Step = (EtlErrorStep)Step,
            Severity = (EtlErrorSeverity)Severity
        };
    }
}
