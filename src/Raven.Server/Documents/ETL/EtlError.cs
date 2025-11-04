using System;

namespace Raven.Server.Documents.ETL;

public class EtlError
{
    public string Id => GetId();
    public DateTime CreatedAt { get; set; }
    public long AffectedDocumentsCount { get; set; }
    public EtlErrorStep Step { get; set; }
    public EtlErrorSeverity Severity { get; set; }
    
    private string GetId()
    {
        return $"{Step}";
    }
}
