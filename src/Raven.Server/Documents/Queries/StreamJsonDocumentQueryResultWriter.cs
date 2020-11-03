using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamJsonDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private readonly AsyncBlittableJsonTextWriter _writer;
        private readonly JsonOperationContext _context;
        private bool _first = true;

        public StreamJsonDocumentQueryResultWriter(Stream stream, JsonOperationContext context)
        {
            _context = context;
            _writer = new AsyncBlittableJsonTextWriter(context, stream);
        }

        public ValueTask DisposeAsync()
        {
            return _writer.DisposeAsync();
        }

        public ValueTask StartResponseAsync()
        {
            return _writer.WriteStartObjectAsync();
        }

        public async ValueTask StartResultsAsync()
        {
            await _writer.WritePropertyNameAsync("Results");
            await _writer.WriteStartArrayAsync();
        }

        public ValueTask EndResultsAsync()
        {
            return _writer.WriteEndArrayAsync();
        }

        public async ValueTask AddResultAsync(Document res)
        {
            if (_first == false)
            {
                await _writer.WriteCommaAsync();
            }
            else
            {
                _first = false;
            }
            await _writer.WriteDocumentAsync(_context, res, metadataOnly: false);
        }

        public ValueTask EndResponseAsync()
        {
            return _writer.WriteEndObjectAsync();
        }

        public async ValueTask WriteErrorAsync(Exception e)
        {
            await _writer.WriteCommaAsync();

            await _writer.WritePropertyNameAsync("Error");
            await _writer.WriteStringAsync(e.ToString());
        }

        public async ValueTask WriteErrorAsync(string error)
        {
            await _writer.WritePropertyNameAsync("Error");
            await _writer.WriteStringAsync(error);
        }

        public async ValueTask WriteQueryStatisticsAsync(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            await _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.ResultEtag));
            await _writer.WriteIntegerAsync(resultEtag);
            await _writer.WriteCommaAsync();

            await _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.IsStale));
            await _writer.WriteBoolAsync(isStale);
            await _writer.WriteCommaAsync();

            await _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.IndexName));
            await _writer.WriteStringAsync(indexName);
            await _writer.WriteCommaAsync();

            await _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.TotalResults));
            await _writer.WriteIntegerAsync(totalResults);
            await _writer.WriteCommaAsync();

            await _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.IndexTimestamp));
            await _writer.WriteStringAsync(timestamp.GetDefaultRavenFormat(isUtc: true));
            await _writer.WriteCommaAsync();
        }

        public bool SupportError => true;
        public bool SupportStatistics => true;
    }
}
