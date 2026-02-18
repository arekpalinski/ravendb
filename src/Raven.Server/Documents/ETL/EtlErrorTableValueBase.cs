using System;

namespace Raven.Server.Documents.ETL;

public abstract class EtlErrorTableValueBase
{
    public string Id => GetId();
    public DateTime CreatedAt;
    public string EtlProcessName;
    public long Step;
    public string Error;
    public string AdditionalInfo;

    protected virtual string GetId() => throw new NotSupportedException();
}
