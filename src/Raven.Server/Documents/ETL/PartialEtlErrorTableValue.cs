using System;

namespace Raven.Server.Documents.ETL;

public class PartialEtlErrorTableValue
{
    public DateTime CreatedAt;
    public string DocumentId;
    public long Step;
    public long Severity;
}
