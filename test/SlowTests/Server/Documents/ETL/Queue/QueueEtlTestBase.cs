using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ETL.Queue;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public abstract class QueueEtlTestBase : RavenTestBase
{
    protected QueueEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected async Task AssertEtlDoneAsync(ManualResetEventSlim etlDone, TimeSpan timeout, string databaseName, QueueEtlConfiguration config)
    {
        if (etlDone.Wait(timeout) == false)
        {
            var loadErrors = await Etl.GetItemLoadErrorsAsync(databaseName, config);
            var transformationErrors = await Etl.GetItemTransformationErrorsAsync(databaseName, config);
            
            Assert.Fail($"ETL wasn't done. Load errors: {loadErrors.First().Error}. Transformation error: {transformationErrors.First().Error}");
        }
    }

}
