using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Elasticsearch
{
    public class ElasticEtlConfiguration : EtlConfiguration<ElasticConnectionString>
    {
        private string _destination;
        
        public ElasticEtlConfiguration()
        {
            ElasticIndexes = new List<ElasticIndex>();
        }
        
        public List<ElasticIndex> ElasticIndexes { get; set; }
        
        public override string GetDestination()
        {
            return _destination ??= $"@{string.Join(",",Connection.Nodes)}";
        }

        public override EtlType EtlType => EtlType.Elastic;
        
        public override bool UsingEncryptedCommunicationChannel()
        {
            foreach (var url in Connection.Nodes)
            {
                if (url.StartsWith("http:", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            return true;
        }

        public override string GetDefaultTaskName()
        {
            return $"Elastic ETL to {ConnectionStringName}";
        }
    }
    
    public class ElasticIndex
    {
        public string IndexName { get; set; }
        public string IndexIdProperty { get; set; }
        public bool InsertOnlyMode { get; set; }

        protected bool Equals(ElasticIndex other)
        {
            return string.Equals(IndexName, other.IndexName) && string.Equals(IndexIdProperty, other.IndexIdProperty, StringComparison.OrdinalIgnoreCase)/* &&
                   InsertOnlyMode == other.InsertOnlyMode*/;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IndexName)] = IndexName,
            };
        }
    }
}
