using System;

namespace Raven.Server.Documents.ETL;

public class PartialEtlErrorTableValue
{
    public string Id => GetId();
    public DateTime CreatedAt;
    public string EtlTaskName;
    public string DocumentId;
    public long Step;
    public long Severity;
    
    private string GetId()
    {
        return $"{EtlTaskName}/{DocumentId}";
    }
    
    public PartialEtlError ToPartialEtlError()
    {
        return new PartialEtlError
        {
            CreatedAt = CreatedAt,
            EtlTaskName = EtlTaskName,
            DocumentId = DocumentId,
            Step = (EtlErrorStep)Step,
            Severity = (EtlErrorSeverity)Severity
        };
    }
}
