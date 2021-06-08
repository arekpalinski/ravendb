using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;
using Raven.Server.Documents.ETL.Providers.Elastic.Enumerators;
using Raven.Server.Documents.ETL.Providers.Elastic.Test;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Elastic
{
    public class ElasticEtl : EtlProcess<ElasticItem, ElasticIndexWithRecords, ElasticEtlConfiguration, ElasticConnectionString>
    {
        public ElasticEtl(Transformation transformation, ElasticEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
            : base(transformation, configuration, database, serverStore, ElasticEtlTag)
        {
        }

        public const string ElasticEtlTag = "ELASTIC ETL";

        public override EtlType EtlType => EtlType.Elastic;

        public override bool ShouldTrackCounters() => false;

        public override bool ShouldTrackTimeSeries() => false;

        protected override bool ShouldTrackAttachmentTombstones() => false;

        protected override bool ShouldFilterOutHiLoDocument() => true;

        protected override IEnumerator<ElasticItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
        {
            return new DocumentsToElasticItems(docs, collection);
        }

        protected override IEnumerator<ElasticItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
            bool trackAttachments)
        {
            return new TombstonesToElasticItems(tombstones, collection);
        }

        protected override IEnumerator<ElasticItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
            List<string> collections)
        {
            throw new NotSupportedException("Attachment tombstones aren't supported by ELASTIC ETL");
        }

        protected override IEnumerator<ElasticItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
            string collection)
        {
            throw new NotSupportedException("Counters aren't supported by ELASTIC ETL");
        }

        protected override IEnumerator<ElasticItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
            string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ELASTIC ETL");
        }

        protected override IEnumerator<ElasticItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
            IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
        {
            throw new NotSupportedException("Time series aren't supported by ELASTIC ETL");
        }

        protected override EtlTransformer<ElasticItem, ElasticIndexWithRecords> GetTransformer(DocumentsOperationContext context)
        {
            return new ElasticDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override int LoadInternal(IEnumerable<ElasticIndexWithRecords> records, DocumentsOperationContext context)
        {
            Uri[] nodes = Configuration.Connection.Nodes.Select(x => new Uri(x)).ToArray();
            var pool = new StaticConnectionPool(nodes);
            var settings = new ConnectionSettings(pool);
            var client = new ElasticClient(settings);
            int statsCounter = 0;
            
            foreach (var index in records)
            {
                StringBuilder deleteQuery = new StringBuilder();
                
                foreach (ElasticItem delete in index.Deletes)
                {
                    deleteQuery.Append($"{delete.DocumentId},");
                }

                var deleteResponse = client.DeleteByQuery<string>(d => d
                    .Index(index.IndexName.ToLower())
                    .Query(q => q
                        .Match(p => p
                            .Field(index.IndexIdProperty)
                            .Query($"{deleteQuery}"))
                    )
                );

                if (deleteResponse.ServerError != null)
                {
                    throw new Exception(deleteResponse.ServerError.Error.ToString());
                }

                statsCounter += (int)deleteResponse.Deleted;

                foreach (ElasticItem insert in index.Inserts)
                {
                    if (insert.Property == null) continue;

                    var response = client.LowLevel.Index<StringResponse>(
                        index: index.IndexName.ToLower(),
                        body: insert.Property.RawValue.ToString(), requestParameters: new IndexRequestParameters(){Refresh = Refresh.WaitFor});

                    if (!response.Success)
                    {
                        throw new Exception(response.OriginalException.Message);
                    }

                    statsCounter++;
                }
            }

            return statsCounter;
        }
        
        public ElasticEtlTestScriptResult RunTest(IEnumerable<ElasticIndexWithRecords> records)
        {
            var simulatedWriter = new ElasticIndexWriterSimulator();
            var summaries = new List<IndexSummary>();
            
            foreach (var record in records)
            {
                var commands = simulatedWriter.SimulateExecuteCommandText(record);
                
                summaries.Add(new IndexSummary
                {
                    IndexName = record.IndexName.ToLower(),
                    Commands = commands.ToArray()
                });
            }
            
            return new ElasticEtlTestScriptResult
            {
                TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                Summary = summaries
            };
        }
    }
}
