namespace Raven.Server.Documents.ETL;

public class EtlItemErrorTableValue : EtlErrorTableValueBase
{
    public string DocumentId;
    
    protected override string GetId()
    {
        return $"{EtlProcessName}/{DocumentId}";
    }
    
    public EtlItemError ToEtlItemError()
    {
        return new EtlItemError
        {
            CreatedAt = CreatedAt,
            EtlProcessName = EtlProcessName,
            DocumentId = DocumentId,
            Step = (EtlErrorStep)Step,
            Error = Error,
            AdditionalInfo = AdditionalInfo
        };
    }
}
