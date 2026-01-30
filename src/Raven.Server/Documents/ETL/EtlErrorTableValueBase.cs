using System;

namespace Raven.Server.Documents.ETL;

public abstract class EtlErrorTableValueBase
{
    public string Id => GetId();
    public DateTime CreatedAt;
    public string EtlProcessName;
    public long Step;
    public long Severity;
    public string Error;

    protected virtual string GetId() => throw new NotSupportedException();
}
