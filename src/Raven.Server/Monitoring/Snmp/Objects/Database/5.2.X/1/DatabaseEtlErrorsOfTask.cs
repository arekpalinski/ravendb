using System;
using System.Globalization;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;


public sealed class DatabaseEtlErrorsOfTask : DatabaseEtlScalarObjectBase<OctetString>
{
    public DatabaseEtlErrorsOfTask(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.EtlErrorsOfTask)
    {
    }
    
    public override ISnmpData Data
    {
        get
        {
            if (Landlord.IsDatabaseLoaded(DatabaseName))
            {
                var database = Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;
                
                database.EtlErrorsStorage.ReadProcessErrorsOfEtl(EtlName, out var processErrors);
                database.EtlErrorsStorage.ReadItemErrorsOfEtl(EtlName, out var itemErrors);

                return new Integer32(processErrors.Count() + itemErrors.Count());
            }

            return null;
        }
    }

    protected override OctetString GetData(DocumentDatabase database)
    {
        throw new NotSupportedException();
    }
}
