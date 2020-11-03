using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class StreamQueryResult<T> : QueryResultServerSide<T>
    {
        private readonly IStreamQueryResultWriter<T> _writer;
        private readonly OperationCancelToken _token;
        private bool _anyWrites;
        private bool _anyExceptions;

        protected StreamQueryResult(HttpResponse response, IStreamQueryResultWriter<T> writer, OperationCancelToken token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");

            _writer = writer;
            _token = token;
        }

        public IStreamQueryResultWriter<T> GetWriter()
        {
            return _writer;
        }

        public OperationCancelToken GetToken()
        {
            return _token;
        }

        public bool HasAnyWrites()
        {
            return _anyWrites;
        }

        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            throw new NotSupportedException();
        }

        public override void AddExplanation(ExplanationResult explanationResult)
        {
            throw new NotSupportedException();
        }

        public override void AddCounterIncludes(IncludeCountersCommand includeCountersCommand)
        {
            throw new NotSupportedException();
        }

        public override void AddTimeSeriesIncludes(IncludeTimeSeriesCommand includeTimeSeriesCommand)
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes()
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> GetTimeSeriesIncludes()
        {
            throw new NotSupportedException();
        }

        public override void AddCompareExchangeValueIncludes(IncludeCompareExchangeValuesCommand command)
        {
            if (command.Results == null)
                return;

            throw new NotSupportedException();
        }

        public override Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> GetCompareExchangeValueIncludes()
        {
            throw new NotSupportedException();
        }

        public override async Task HandleExceptionAsync(Exception e)
        {
            await StartResponseIfNeededAsync();

            _anyExceptions = true;

            await _writer.EndResultsAsync();
            await _writer.WriteErrorAsync(e);

            throw e;
        }

        public async Task StartResponseIfNeededAsync()
        {
            if (_anyWrites)
                return;

            await StartResponse();

            _anyWrites = true;
        }

        public override bool SupportsExceptionHandling => true;

        public override bool SupportsInclude => false;
        public override bool SupportsHighlighting => false;
        public override bool SupportsExplanations => false;

        public async Task FlushAsync()// intentionally not using Disposable here, because we need better error handling
        {
            await StartResponseIfNeededAsync();

            await EndResponseAsync();
        }

        private async Task StartResponse()
        {
            await _writer.StartResponseAsync();

            if (_writer.SupportStatistics)
            {
                await _writer.WriteQueryStatisticsAsync(ResultEtag, IsStale, IndexName, TotalResults, IndexTimestamp);
            }
            await _writer.StartResultsAsync();
        }

        private async Task EndResponseAsync()
        {
            if (_anyExceptions == false)
                await _writer.EndResultsAsync();

            await _writer.EndResponseAsync();
        }
    }
}
