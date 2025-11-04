using System;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL;

public class PartialEtlError
{
    public string Id => GetId();
    public DateTime CreatedAt { get; set; }
    public string DocumentId { get; set; }
    public EtlErrorStep Step { get; set; }
    public EtlErrorSeverity Severity { get; set; }
    
    private string GetId()
    {
        return $"{Step}";
    }
}
