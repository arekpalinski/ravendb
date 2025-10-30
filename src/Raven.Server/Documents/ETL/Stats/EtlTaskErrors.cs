using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats;

public sealed class EtlTaskErrors : IDynamicJson
{
    public string TaskName { get; set; }

    public EtlProcessTransformationDebugStats[] Errors { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(TaskName)] = TaskName,
            [nameof(Errors)] = new DynamicJsonArray(Errors.Select(x => x.ToJson()))
        };
    }
}
