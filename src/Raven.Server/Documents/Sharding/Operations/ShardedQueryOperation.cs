﻿using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedQueryOperation : IShardedReadOperation<QueryResult, ShardedQueryResult>
{
    private readonly TransactionOperationContext _context;
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private readonly Dictionary<int, ShardedQueryCommand> _queryCommands;
    private readonly ShardedDocumentsComparer _sortingComparer;
    private long _combinedResultEtag;

    public ShardedQueryOperation(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, Dictionary<int, ShardedQueryCommand> queryCommands, ShardedDocumentsComparer sortingComparer, string expectedEtag)
    {
        _context = context;
        _requestHandler = requestHandler;
        _queryCommands = queryCommands;
        _sortingComparer = sortingComparer;
        ExpectedEtag = expectedEtag;
    }

    public string ExpectedEtag { get; }

    public HttpRequest HttpRequest { get => _requestHandler.HttpContext.Request; }

    RavenCommand<QueryResult> IShardedOperation<QueryResult, ShardedReadResult<ShardedQueryResult>>.CreateCommandForShard(int shardNumber) => _queryCommands[shardNumber];

    public HashSet<string> MissingDocumentIncludes { get; private set; }

    public HashSet<string> MissingCounterIncludes { get; private set; }

    public Dictionary<string, List<TimeSeriesRange>> MissingTimeSeriesIncludes { get; set; }

    public string CombineCommandsEtag(Memory<RavenCommand<QueryResult>> commands)
    {
        _combinedResultEtag = 0;

        foreach (var cmd in commands.Span)
        {
            _combinedResultEtag = Hashing.Combine(_combinedResultEtag, cmd.Result.ResultEtag);
        }

        return CharExtensions.ToInvariantString(_combinedResultEtag);
    }

    public ShardedQueryResult CombineResults(Memory<QueryResult> results)
    {
        var result = new ShardedQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
            ResultEtag = _combinedResultEtag
        };

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Check if we could handle this in streaming manner so we won't need to materialize all blittables and do Clone() here");

        ShardedRevisionIncludes revisionIncludes = null;
        ShardedCounterIncludes counterIncludes = null;
        ShardedCompareExchangeValueInclude compareExchangeValueIncludes = null;
        ShardedTimeSeriesIncludes timeSeriesIncludes = null;

        foreach (var cmdResult in results.Span)
        {
            result.TotalResults += cmdResult.TotalResults;
            result.IsStale |= cmdResult.IsStale;
            result.SkippedResults += cmdResult.SkippedResults;
            result.IndexName = cmdResult.IndexName;
            result.IncludedPaths = cmdResult.IncludedPaths;

            if (result.IndexTimestamp < cmdResult.IndexTimestamp) 
                result.IndexTimestamp = cmdResult.IndexTimestamp;

            if (result.LastQueryTime < cmdResult.LastQueryTime) 
                result.LastQueryTime = cmdResult.LastQueryTime;

            if (cmdResult.RaftCommandIndex.HasValue)
            {
                if (result.RaftCommandIndex == null || cmdResult.RaftCommandIndex > result.RaftCommandIndex)
                    result.RaftCommandIndex = cmdResult.RaftCommandIndex;
            }

            // For includes, we send the includes to all shards, then we merge them together. We do explicitly
            // support including from another shard, so we'll need to do that again for missing includes
            // That means also recording the include() call from JS on missing values that we'll need to rerun on
            // other shards

            if (cmdResult.Includes is { Count: > 0 })
            {
                result.Includes ??= new List<BlittableJsonReaderObject>();

                HandleDocumentIncludes(cmdResult, ref result);
            }

            if (cmdResult.RevisionIncludes is {Length: > 0})
            {
                revisionIncludes ??= new ShardedRevisionIncludes();

                revisionIncludes.AddResults(cmdResult.RevisionIncludes, _context);
            }

            if (cmdResult.CounterIncludes != null)
            {
                counterIncludes ??= new ShardedCounterIncludes();

                counterIncludes.AddResults(cmdResult.CounterIncludes, cmdResult.IncludedCounterNames, _context);
            }

            if (cmdResult.TimeSeriesIncludes != null)
            {
                timeSeriesIncludes ??= new ShardedTimeSeriesIncludes(true);

                timeSeriesIncludes.AddResults(cmdResult.TimeSeriesIncludes, _context);
            }

            if (cmdResult.CompareExchangeValueIncludes != null)
            {
                compareExchangeValueIncludes ??= new ShardedCompareExchangeValueInclude();

                compareExchangeValueIncludes.AddResults(cmdResult.CompareExchangeValueIncludes, _context);
            }

            if (_sortingComparer == null)
            {
                foreach (BlittableJsonReaderObject item in cmdResult.Results)
                {
                    result.Results.Add(item.Clone(_context));
                }
            }
        }

        if (revisionIncludes != null)
            result.AddRevisionIncludes(revisionIncludes);

        if (counterIncludes != null)
        {
            result.AddCounterIncludes(counterIncludes);
            MissingCounterIncludes = counterIncludes.MissingCounterIncludes;
        }

        if (timeSeriesIncludes != null)
        {
            result.AddTimeSeriesIncludes(timeSeriesIncludes);
            MissingTimeSeriesIncludes = timeSeriesIncludes.MissingTimeSeriesIncludes;
        }

        if (compareExchangeValueIncludes != null)
            result.AddCompareExchangeValueIncludes(compareExchangeValueIncludes);

        if (_sortingComparer != null)
        {
            // all the results from each command are already ordered

            using (var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(_sortingComparer))
            {
                foreach (var cmdResult in results.Span)
                {
                    mergedEnumerator.AddEnumerator(GetEnumerator(cmdResult.Results));
                }

                while (mergedEnumerator.MoveNext())
                {
                    result.Results.Add(mergedEnumerator.Current?.Clone(_context));
                }

                static IEnumerator<BlittableJsonReaderObject> GetEnumerator(BlittableJsonReaderArray array)
                {
                    foreach (BlittableJsonReaderObject item in array)
                    {
                        yield return item;
                    }
                }
            }
        }

        return result;
    }

    private void HandleDocumentIncludes(QueryResult cmdResult, ref ShardedQueryResult result)
    {
        foreach (var id in cmdResult.Includes.GetPropertyNames())
        {
            if (cmdResult.Includes.TryGet(id, out BlittableJsonReaderObject include) && include != null)
            {
                result.Includes.Add(include.Clone(_context));
            }
            else
            {
                (MissingDocumentIncludes ??= new HashSet<string>()).Add(id);
            }
        }
    }
}