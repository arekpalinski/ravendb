using System;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class DatabaseEtlLastSuccessfulBatchTime : DatabaseEtlScalarObjectBase<TimeTicks>
{
    public DatabaseEtlLastSuccessfulBatchTime(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.LastSuccessfulBatchTime)
    {
    }
    
    public override ISnmpData Data
    {
        get
        {
            if (Landlord.IsDatabaseLoaded(DatabaseName))
            {
                var database = Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;

                var lastSuccessfulBatchTime = database.EtlLoader.Processes.Single(x => x.Name == EtlName).Statistics.LastSuccessfulBatchTime;

                if (lastSuccessfulBatchTime.HasValue)
                    return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - lastSuccessfulBatchTime.Value);
            }

            return null;
        }
    }

    protected override TimeTicks GetData(DocumentDatabase database)
    {
        throw new NotSupportedException();
    }
}
