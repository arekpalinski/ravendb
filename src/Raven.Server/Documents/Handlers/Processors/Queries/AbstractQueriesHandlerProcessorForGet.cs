﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult> : AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TQueryContext : IDisposable
{
    protected AbstractQueriesHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache, HttpMethod method) : base(requestHandler, queryMetadataCache)
    {
        QueryMethod = method;
    }

    protected abstract IDisposable AllocateContextForQueryOperation(out TQueryContext queryContext, out TOperationContext context);

    protected abstract ValueTask HandleDebugAsync(IndexQueryServerSide query, TQueryContext queryContext, string debug, long? existingResultEtag, OperationCancelToken token);

    protected abstract ValueTask HandleFacetedQueryAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract ValueTask<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract ValueTask<QueryResultServerSide<TQueryResult>> GetQueryResultsAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag,
        bool metadataOnly,
        OperationCancelToken token);

    protected override HttpMethod QueryMethod { get; }

    public override async ValueTask ExecuteAsync()
    {
        using (var tracker = CreateRequestTimeTracker())
        {
            try
            {
                using (var token = RequestHandler.CreateTimeLimitedQueryToken())
                using (AllocateContextForQueryOperation(out var queryContext, out var context))
                {
                    var addSpatialProperties = RequestHandler.GetBoolValueQueryString("addSpatialProperties", required: false) ?? false;
                    var returnMissingIncludeAsNull = RequestHandler.GetBoolFromHeaders(Constants.Headers.Sharded) ?? false;
                    var metadataOnly = RequestHandler.GetBoolValueQueryString("metadataOnly", required: false) ?? false;
                    var shouldReturnServerSideQuery = RequestHandler.GetBoolValueQueryString("includeServerSideQuery", required: false) ?? false;

                    var indexQuery = await GetIndexQueryAsync(context, QueryMethod, tracker, addSpatialProperties, returnMissingIncludeAsNull);

                    indexQuery.Diagnostics = RequestHandler.GetBoolValueQueryString("diagnostics", required: false) ?? false ? new List<string>() : null;
                    indexQuery.AddTimeSeriesNames = RequestHandler.GetBoolValueQueryString("addTimeSeriesNames", false) ?? false;
                    indexQuery.DisableAutoIndexCreation = RequestHandler.GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

                    var existingResultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

                    var debug = RequestHandler.GetStringQueryString("debug", required: false);

                    EnsureQueryContextInitialized(queryContext, indexQuery);

                    if (string.IsNullOrWhiteSpace(debug) == false)
                    {
                        await HandleDebugAsync(indexQuery, queryContext, debug, existingResultEtag, token);
                        return;
                    }

                    if (TrafficWatchManager.HasRegisteredClients)
                        RequestHandler.TrafficWatchQuery(indexQuery);

                    if (indexQuery.Metadata.HasFacet)
                    {
                        await HandleFacetedQueryAsync(indexQuery, queryContext, existingResultEtag, token);
                        return;
                    }

                    if (indexQuery.Metadata.HasSuggest)
                    {
                        await HandleSuggestQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
                        return;
                    }

                    QueryResultServerSide<TQueryResult> result;
                    try
                    {
                        result = await GetQueryResultsAsync(indexQuery, queryContext, existingResultEtag, metadataOnly, token);
                    }
                    catch (IndexDoesNotExistException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    if (result.NotModified)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;
                    }

                    HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

                    long numberOfResults;
                    long totalDocumentsSizeInBytes;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), HttpContext.RequestAborted))
                    {
                        result.Timings = indexQuery.Timings?.ToTimings();

                        (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentQueryResultAsync(context, result, metadataOnly, WriteAdditionalData(indexQuery, shouldReturnServerSideQuery), token.Token);
                        await writer.OuterFlushAsync();
                    }


                    QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);

                    if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
                    {
                        RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"Query ({result.IndexName})",
                            $"{indexQuery.Metadata.QueryText}\n{indexQuery.QueryParameters}", numberOfResults, indexQuery.PageSize, result.DurationInMs,
                            totalDocumentsSizeInBytes);
                    }
                }
            }
            catch (Exception e)
            {
                if (tracker.Query == null)
                {
                    string errorMessage;
                    if (e is EndOfStreamException || e is ArgumentException)
                    {
                        errorMessage = $"Failed: {e.Message}";
                    }
                    else
                    {
                        errorMessage = $"Failed: {HttpContext.Request.Path.Value} {e}";
                    }

                    tracker.Query = errorMessage;

                    if (TrafficWatchManager.HasRegisteredClients)
                        RequestHandler.AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
                }
                throw;
            }
        }
    }

    protected virtual void EnsureQueryContextInitialized(TQueryContext queryContext, IndexQueryServerSide indexQuery)
    {

    }

    private Action<AbstractBlittableJsonTextWriter> WriteAdditionalData(IndexQueryServerSide indexQuery, bool shouldReturnServerSideQuery)
    {
        if (indexQuery.Diagnostics == null && shouldReturnServerSideQuery == false)
            return null;

        return w =>
        {
            if (shouldReturnServerSideQuery)
            {
                w.WriteComma();
                w.WritePropertyName(nameof(indexQuery.ServerSideQuery));
                w.WriteString(indexQuery.ServerSideQuery);
            }

            if (indexQuery.Diagnostics != null)
            {
                w.WriteComma();
                w.WriteArray(nameof(indexQuery.Diagnostics), indexQuery.Diagnostics);
            }
        };
    }

    private async ValueTask HandleSuggestQueryAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext operationContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await GetSuggestionQueryResultAsync(query, queryContext, existingResultEtag, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(operationContext, RequestHandler.ResponseBodyStream()))
        {
            (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteSuggestionQueryResultAsync(operationContext, result, token.Token);
        }

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"SuggestQuery ({result.IndexName})", query.Query, numberOfResults, query.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);
    }
}