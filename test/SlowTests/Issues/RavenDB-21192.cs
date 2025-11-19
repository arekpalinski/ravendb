using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;
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
                EtlTaskName = "ETL1",
                AffectedDocumentsCount = 1,
                Step = EtlErrorStep.TransformationError,
                Severity = EtlErrorSeverity.Low
            };

            var error2 = new EtlError()
            {
                CreatedAt = now.AddDays(1),
                EtlTaskName = "ETL2",
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
                Assert.Equal(error1.EtlTaskName, errors[0].EtlTaskName);
                Assert.Equal(error1.AffectedDocumentsCount, errors[0].AffectedDocumentsCount);
                Assert.Equal((long)error1.Step, errors[0].Step);
                Assert.Equal((long)error1.Severity, errors[0].Severity);
                
                Assert.Equal(error2.CreatedAt, errors[1].CreatedAt);
                Assert.Equal(error2.EtlTaskName, errors[1].EtlTaskName);
                Assert.Equal(error2.AffectedDocumentsCount, errors[1].AffectedDocumentsCount);
                Assert.Equal((long)error2.Step, errors[1].Step);
                Assert.Equal((long)error2.Severity, errors[1].Severity);
            }
                
            var partialError1 = new PartialEtlError()
            {
                DocumentId = "doc/1", 
                EtlTaskName = "ETL1",
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
                Assert.Equal(partialError1.EtlTaskName, partialErrors[0].EtlTaskName);
                Assert.Equal(partialError1.DocumentId, partialErrors[0].DocumentId);
                Assert.Equal((long)partialError1.Step, partialErrors[0].Step);
                Assert.Equal((long)partialError1.Severity, partialErrors[0].Severity);
            }

            using (database.EtlErrorsStorage.ReadErrorsOfTask("ETL1", out var firstTaskErrors))
            {
                var errors = firstTaskErrors.ToList();
                
                Assert.Single(errors);
                
                Assert.Equal(error1.CreatedAt, errors[0].CreatedAt);
                Assert.Equal(error1.EtlTaskName, errors[0].EtlTaskName);
                Assert.Equal(error1.AffectedDocumentsCount, errors[0].AffectedDocumentsCount);
                Assert.Equal((long)error1.Step, errors[0].Step);
                Assert.Equal((long)error1.Severity, errors[0].Severity);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void Test()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe")
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };

            AddEtlTask(src, dest, etlName1, connectionStringName1, transformationName1, script1, collections1);

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 50; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 900; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 900; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 1000; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            WaitForUserToContinueTheTest(src);
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestEtlErrorsEndpoint()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   this.Name = 'James Doe';
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            const string connectionStringName2 = "ConnectionString2";
            const string etlName2 = "ETL2";
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   this.Name = 'Cool Company';
                                   loadToCompanies(this);
                                   """;
            var collections2 = new List<string>() { "Companies" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, transformationName1, script1, collections1);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 5; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe" });
            }
            
            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadErrors >= 1);
            
            AddEtlTask(src, dest, etlName2, connectionStringName2, transformationName2, script2, collections2);
            
            var result = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true)).GetAwaiter().GetResult();
            
            Assert.True(result.Disabled);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 5; i++)
                    bulkInsert.Store(new Company() { Name = "Some Company" });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            using (var commands = src.Commands())
            {
                var cmd = new GetEtlTaskErrorsCommand(new List<string>() { etlName1, etlName2 });
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet(nameof(EtlHandlerProcessorForErrors.Response.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<EtlTaskErrors>>(results.ToString());

                var firstTaskErrors = resultsObjectList.Single(x => x.TaskName == "ETL1/Transformation1");
                
                Assert.Empty(firstTaskErrors.Errors);
                Assert.Equal(5, firstTaskErrors.PartialErrors.Length);
                
                var secondTaskErrors = resultsObjectList.Single(x => x.TaskName == "ETL2/Transformation2");
                
                Assert.True(secondTaskErrors.Errors.Any(x => x.AffectedDocumentsCount == 5));
                Assert.Empty(secondTaskErrors.PartialErrors);                
            }
            
            WaitForUserToContinueTheTest(src);
        }
    }

    private static void AddEtlTask(DocumentStore src, DocumentStore dest, string etlName, string connectionStringName, string transformationName, string script, List<string> collections)
    {
        var transformation1 = new Transformation
        {
            Name = transformationName,
            Collections = collections,
            Script = script,
            ApplyToAllDocuments = false,
            Disabled = false
        };

        var configuration1 = new RavenEtlConfiguration
        {
            Name = etlName,
            ConnectionStringName = connectionStringName,
            Transforms =
            {
                transformation1
            },
            MentorNode = null,
            PinToMentorNode = false
        };

        var connectionString1 = new RavenConnectionString
        {
            Name = connectionStringName,
            Database = dest.Database,
            TopologyDiscoveryUrls = dest.Urls
        };

        src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString1));
        src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration1));
    }

    private class User
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private class Company
    {
        public string Name { get; set; }
    }
    
    private class GetEtlTaskErrorsCommand : RavenCommand<object>
    {
        private List<string> _taskNames;
        
        public GetEtlTaskErrorsCommand(List<string> taskNames)
        {
            _taskNames = taskNames;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/etl/errors?{string.Join(',', _taskNames)}";
            
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }

        public override bool IsReadRequest => true;
    }
}
