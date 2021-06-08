using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Elastic
{
    internal class ElasticDocumentTransformer : EtlTransformer<ElasticItem, ElasticIndexWithRecords>
    {
        private readonly Transformation _transformation;
        private readonly ElasticEtlConfiguration _config;
        private readonly Dictionary<string, ElasticIndexWithRecords> _indexes;
        private Dictionary<string, Queue<Attachment>> _loadedAttachments;
        private readonly List<ElasticIndex> _indexesForScript;

        private EtlStatsScope _stats;

        public ElasticDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ElasticEtlConfiguration config)
            : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.ElasticEtl), null)
        {
            _transformation = transformation;
            _config = config;

            var destinationIndexes = transformation.GetCollectionsFromScript();

            LoadToDestinations = destinationIndexes;

            _indexes = new Dictionary<string, ElasticIndexWithRecords>(destinationIndexes.Length);
            _indexesForScript = new List<ElasticIndex>(destinationIndexes.Length);

            for (var i = 0; i < _config.ElasticIndexes.Count; i++)
            {
                var table = _config.ElasticIndexes[i];

                if (destinationIndexes.Contains(table.IndexName, StringComparer.OrdinalIgnoreCase))
                    _indexesForScript.Add(table);
            }
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            if (DocumentScript == null)
                return;
        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
            throw new System.NotImplementedException();
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            throw new NotSupportedException("Counters aren't supported by ELASTIC ETL");
        }

        protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            throw new NotSupportedException("Time series aren't supported by ELASTIC ETL");
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string indexName, ScriptRunnerResult document)
        {
            if (indexName == null)
                ThrowLoadParameterIsMandatory(nameof(indexName));

            var result = document.TranslateToObject(Context);
            var property = new ElasticProperty {RawValue = result};

            GetOrAdd(indexName).Inserts.Add(new ElasticItem(Current) {Property = property});

            //_stats.IncrementBatchSize(result.Size);
        }

        public override List<ElasticIndexWithRecords> GetTransformedResults()
        {
            return _indexes.Values.ToList();
        }

        public override void Transform(ElasticItem item, EtlStatsScope stats, EtlProcessState state)
        {
            if (item.IsDelete == false)
            {
                Current = item;
                DocumentScript.Run(Context, Context, "execute", new object[] {Current.Document}).Dispose();
            }

            for (int i = 0; i < _indexesForScript.Count; i++)
            {
                // delete all the rows that might already exist there
                var elasticIndex = _indexesForScript[i];

                GetOrAdd(elasticIndex.IndexName).Deletes.Add(item);
            }
        }

        private ElasticIndexWithRecords GetOrAdd(string indexName)
        {
            if (_indexes.TryGetValue(indexName, out ElasticIndexWithRecords index) == false)
            {
                var elasticIndex = _config.ElasticIndexes.Find(x => x.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase));

                if (elasticIndex == null)
                    ThrowTableNotDefinedInConfig(indexName);

                _indexes[indexName] =
                    index = new ElasticIndexWithRecords(elasticIndex);
            }

            return index;
        }

        private static void ThrowTableNotDefinedInConfig(string indexName)
        {
            throw new InvalidOperationException($"Table '{indexName}' was not defined in the configuration of ELASTIC ETL task");
        }
    }
}
