using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Elastic.Test
{
    public class IndexSummary
    {
        public string IndexName { get; set; }

        public string[] Commands { get; set; }
    }
}
