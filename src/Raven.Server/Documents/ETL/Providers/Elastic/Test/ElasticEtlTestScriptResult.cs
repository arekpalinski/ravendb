using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.Elastic.Test
{
    public class ElasticEtlTestScriptResult : TestEtlScriptResult
    {
        public List<IndexSummary> Summary { get; set; }
    }
}
