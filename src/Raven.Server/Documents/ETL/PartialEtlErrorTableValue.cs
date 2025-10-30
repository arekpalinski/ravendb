using System;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL;

public class PartialEtlErrorTableValue
{
    public DateTime CreatedAt;
    public LazyStringValue DocumentId;
    public long Type;
    public long Severity;
}
