using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Handlers.Batching
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkDocs()
        {
            using (var processor = new BatchHandlerProcessorForBulkDocs(this))
                await processor.ExecuteAsync();
        }
    }
}
