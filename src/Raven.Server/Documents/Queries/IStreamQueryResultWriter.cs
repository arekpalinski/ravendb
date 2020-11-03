using System;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries
{
    public interface IStreamQueryResultWriter<in T> : IAsyncDisposable
    {
        ValueTask StartResponseAsync();
        ValueTask StartResultsAsync();
        ValueTask EndResultsAsync();
        ValueTask AddResultAsync(T res);
        ValueTask EndResponseAsync();
        ValueTask WriteErrorAsync(Exception e);
        ValueTask WriteErrorAsync(string error);
        ValueTask WriteQueryStatisticsAsync(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp);
        bool SupportStatistics { get; }
    }
}
