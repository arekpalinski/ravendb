using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentIndexEntriesQueryResult : StreamQueryResult<BlittableJsonReaderObject>
    {
        public override async ValueTask AddResultAsync(BlittableJsonReaderObject result)
        {
            if (HasAnyWrites() == false)
                await StartResponseIfNeededAsync();

            using (result)
                await GetWriter().AddResultAsync(result);
            GetToken().Delay();
        }

        public StreamDocumentIndexEntriesQueryResult(HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token) : base(response, writer, token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");
        }
    }
}
