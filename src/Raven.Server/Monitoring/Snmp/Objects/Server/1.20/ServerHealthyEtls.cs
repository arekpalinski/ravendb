using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public class ServerHealthyEtls : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;
    
    public ServerHealthyEtls(ServerStore store)
        : base(SnmpOids.Server.NumberOfHealthyEtls)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;
        
        foreach (var db in _store.DatabasesLandlord.DatabasesCache)
        {
            result += db.Value.GetAwaiter().GetResult().EtlLoader.Processes.Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Healthy);
        }
        
        return new Integer32(result);
    }
}
