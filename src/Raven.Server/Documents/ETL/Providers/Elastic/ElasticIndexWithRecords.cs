using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;

namespace Raven.Server.Documents.ETL.Providers.Elastic
{
    public class ElasticIndexWithRecords : ElasticIndex
    {
        public readonly List<ElasticItem> Deletes = new List<ElasticItem>();

        public readonly List<ElasticItem> Inserts = new List<ElasticItem>();

        public ElasticIndexWithRecords(ElasticIndex index)
        {
            IndexName = index.IndexName;
            IndexIdProperty = index.IndexIdProperty;
            InsertOnlyMode = index.InsertOnlyMode;
        }
    }
}
