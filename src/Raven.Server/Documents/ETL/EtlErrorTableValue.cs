using System;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL;

public class EtlErrorTableValue
{
    public DateTime CreatedAt;
    public long AffectedDocumentsCount;
    public long Type;
    public long Severity;
}
