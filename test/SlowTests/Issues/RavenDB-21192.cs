using System;
using FastTests;
using Raven.Server.Documents.ETL;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21192 : RavenTestBase
{
    public RavenDB_21192(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void Test()
    {
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).Result;

            var now = DateTime.Now;

            var error1 = new EtlError()
            {
                CreatedAt = now,
                AffectedDocumentsCount = 1,
                Type = EtlErrorType.TransformationError,
                Severity = EtlErrorSeverity.Low
            };

            var error2 = new EtlError()
            {
                CreatedAt = now,
                AffectedDocumentsCount = 21,
                Type = EtlErrorType.LoadError,
                Severity = EtlErrorSeverity.High
            };
                
            database.EtlErrorsStorage.StoreError(error1);
            database.EtlErrorsStorage.StoreError(error2);

            var x = database.EtlErrorsStorage.ReadError("id1");
                
            var partialError1 = new PartialEtlError()
            {
                DocumentId = "doc/1", 
                CreatedAt = now,
                Type = EtlErrorType.LoadError,
                Severity = EtlErrorSeverity.Low
            };
                
                
            database.EtlErrorsStorage.StorePartialError(partialError1);
        }
    }
}
