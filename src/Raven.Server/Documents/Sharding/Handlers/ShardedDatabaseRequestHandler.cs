﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Configuration;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public partial class ShardedDatabaseRequestHandler : AbstractDatabaseRequestHandler<TransactionOperationContext>
    {
        public ShardedDatabaseContext DatabaseContext;
        public Logger Logger;

        public HttpMethod Method;
        public string RelativeShardUrl;
        public string BaseShardUrl;
        public ShardExecutor ShardExecutor => DatabaseContext.ShardExecutor;

        public ShardedContinuationTokensHandler ContinuationTokens;

        public static List<string> HeadersToCopy = new List<string> { Constants.Headers.LastKnownClusterTransactionIndex };

        public ShardedDatabaseRequestHandler()
        {
            ContinuationTokens = new ShardedContinuationTokensHandler(this);
        }

        private void ModifyHeaders(HttpRequestMessage request)
        {
            foreach (var header in HeadersToCopy)
            {
                if (HttpContext.Request.Headers.TryGetValue(header, out var value))
                {
                    request.Headers.TryAddWithoutValidation(header, (IEnumerable<string>)value);
                }
            }
        }

        public Task<TResult> ExecuteSingleShardAsync<TResult>(JsonOperationContext context, RavenCommand<TResult> command, int shardNumber, CancellationToken token = default)
        {
            command.ModifyRequest = ModifyHeaders;
            return ShardExecutor.ExecuteSingleShardAsync(context, command, shardNumber, token);
        }


        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            DatabaseContext = context.DatabaseContext;
            //TODO - sharding: We probably want to put it in the ShardedDatabaseContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(DatabaseContext.DatabaseName, GetType().FullName);

            var request = HttpContext.Request;
            var url = request.Path.Value;
            var relativeIndex = url.IndexOf('/', 11); //start after "/databases/" and skip the database name

            BaseShardUrl = url.Substring(relativeIndex);
            RelativeShardUrl = BaseShardUrl + request.QueryString;
            Method = new HttpMethod(request.Method);

            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }


        public Task CheckForChanges(RequestHandlerContext context)
        {
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && DatabaseContext.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ClientConfigurationHelper.HasClientConfigurationChanged(DatabaseContext.DatabaseRecord.Client, ServerStore, clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            return Task.CompletedTask;
        }

        public override async Task WaitForIndexToBeAppliedAsync(TransactionOperationContext context, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index);

            using (var cts = CreateOperationToken())
            {
                await DatabaseContext.RachisLogIndexNotifications.WaitForIndexNotification(index, cts.Token);

                var dbs = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(DatabaseContext.DatabaseName).ToList();

                foreach (var task in dbs)
                {
                    var database = await task.WithCancellation(cts.Token);
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout).WithCancellation(cts.Token);
                }
            }
        }

        public override OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(DatabaseContext.DatabaseShutdown, HttpContext.RequestAborted);
        }

        protected override OperationCancelToken CreateOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, DatabaseContext.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override OperationCancelToken CreateTimeLimitedOperationToken()
        {
            return new OperationCancelToken(DatabaseContext.Configuration.Databases.OperationTimeout.AsTimeSpan, DatabaseContext.DatabaseShutdown, HttpContext.RequestAborted);
        }

        public override string DatabaseName => DatabaseContext.DatabaseName;

        public override Task WaitForIndexNotificationAsync(long index) => ServerStore.Cluster.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
        public override bool ShouldAddPagingPerformanceHint(long numberOfResults)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "implement this");
            return false;
        }

        public override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "implement this");
        }
    }
}
