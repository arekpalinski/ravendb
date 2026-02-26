using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseEtlErrors : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseEtlErrors(string databaseName, DatabasesLandlord landlord, int index) : base(databaseName, landlord, SnmpOids.Databases.EtlErrors, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(GetCount(database));
    }
    
    private static int GetCount(DocumentDatabase database)
    {
        database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrors);
        database.EtlErrorsStorage.ReadAllProcessErrors(out var processErrors);
        
        return itemErrors.Count + processErrors.Count;
    }
}
