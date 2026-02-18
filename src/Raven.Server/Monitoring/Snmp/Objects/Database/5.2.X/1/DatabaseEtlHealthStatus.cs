using System;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseEtlHealthStatus : DatabaseEtlScalarObjectBase<OctetString>
{
    public DatabaseEtlHealthStatus(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.HealthStatus)
    {
    }
    
    public override ISnmpData Data
    {
        get
        {
            if (Landlord.IsDatabaseLoaded(DatabaseName))
            {
                var database = Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;

                var healthStatus = database.EtlLoader.Processes.Single(x => x.Name == EtlName).Statistics.HealthStatus;

                return new OctetString(healthStatus.ToString());
            }

            return null;
        }
    }

    protected override OctetString GetData(DocumentDatabase database)
    {
        throw new NotSupportedException();
    }
}
