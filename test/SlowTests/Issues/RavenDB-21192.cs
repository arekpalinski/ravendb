using System;
using System.Linq;
using FastTests;
using Raven.Server.Documents.ETL;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21192 : RavenTestBase
{
    public RavenDB_21192(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestEtlErrorsStorage()
    {
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).Result;

            var now = DateTime.Now;

            var error1 = new EtlError()
            {
                CreatedAt = now,
                AffectedDocumentsCount = 1,
                Step = EtlErrorStep.TransformationError,
                Severity = EtlErrorSeverity.Low
            };

            var error2 = new EtlError()
            {
                CreatedAt = now.AddDays(1),
                AffectedDocumentsCount = 21,
                Step = EtlErrorStep.LoadError,
                Severity = EtlErrorSeverity.High
            };
                
            database.EtlErrorsStorage.StoreError(error1);
            database.EtlErrorsStorage.StoreError(error2);

            using (database.EtlErrorsStorage.ReadErrorsOrderedByCreationDate(out var errorTableValues))
            {
                var errors = errorTableValues.ToList();
                
                Assert.Equal(2, errors.Count);
                
                Assert.Equal(error1.CreatedAt, errors[0].CreatedAt);
                Assert.Equal(error1.AffectedDocumentsCount, errors[0].AffectedDocumentsCount);
                Assert.Equal((long)error1.Step, errors[0].Step);
                Assert.Equal((long)error1.Severity, errors[0].Severity);
                
                Assert.Equal(error2.CreatedAt, errors[1].CreatedAt);
                Assert.Equal(error2.AffectedDocumentsCount, errors[1].AffectedDocumentsCount);
                Assert.Equal((long)error2.Step, errors[1].Step);
                Assert.Equal((long)error2.Severity, errors[1].Severity);
            }
                
            var partialError1 = new PartialEtlError()
            {
                DocumentId = "doc/1", 
                CreatedAt = now,
                Step = EtlErrorStep.LoadError,
                Severity = EtlErrorSeverity.Low
            };
            
            database.EtlErrorsStorage.StorePartialError(partialError1);
            
            using (database.EtlErrorsStorage.ReadPartialErrorsOrderedByCreationDate(out var partialErrorTableValues))
            {
                var partialErrors = partialErrorTableValues.ToList();

                Assert.Single(partialErrors);
                
                Assert.Equal(partialError1.CreatedAt, partialErrors[0].CreatedAt);
                Assert.Equal(partialError1.DocumentId, partialErrors[0].DocumentId);
                Assert.Equal((long)partialError1.Step, partialErrors[0].Step);
                Assert.Equal((long)partialError1.Severity, partialErrors[0].Severity);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestEtlErrorsEndpoint()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            Etl.AddEtl(src, dest, "Users", script: """
                                                   this.Name = 'James Doe';
                                                   throw new Error("dummy error");
                                                   loadToUsers(this);
                                                   """);
            
            //var etlDone = Etl.WaitForEtlToComplete(src);

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 1000; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe" });
            }
            
            /*
            using (var session = src.OpenSession())
            {
                session.Store(new User { Name = "Joe Doe" });
                session.SaveChanges();
            }
            */
            
            //etlDone.Wait(TimeSpan.FromSeconds(15));
            
            WaitForUserToContinueTheTest(src);
        }
    }

    private class User
    {
        public string Name { get; set; }
    }
}
