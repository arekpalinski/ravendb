using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats;

public sealed class EtlErrors : IDynamicJson
{
    public string ProcessName { get; set; }

    public EtlProcessError[] ProcessErrors { get; set; }
    
    public EtlItemError[] ItemErrors { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ProcessName)] = ProcessName,
            [nameof(ProcessErrors)] = new DynamicJsonArray(ProcessErrors.Select(x => x.ToJson())),
            [nameof(ItemErrors)] = new DynamicJsonArray(ItemErrors.Select(x => x.ToJson()))
        };
    }
}
