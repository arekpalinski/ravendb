using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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
                Severity = EtlErrorSeverity.Low,
                Message = "Test message"
            };

            var error2 = new EtlError()
            {
                CreatedAt = now.AddDays(1),
                EtlTaskName = "ETL2",
                AffectedDocumentsCount = 21,
                Step = EtlErrorStep.LoadError,
                Severity = EtlErrorSeverity.High,
                Message = "Test message"
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
                Assert.Equal(error1.Message, errors[0].Message);
                
                Assert.Equal(error2.CreatedAt, errors[1].CreatedAt);
                Assert.Equal(error2.EtlTaskName, errors[1].EtlTaskName);
                Assert.Equal(error2.AffectedDocumentsCount, errors[1].AffectedDocumentsCount);
                Assert.Equal((long)error2.Step, errors[1].Step);
                Assert.Equal((long)error2.Severity, errors[1].Severity);
                Assert.Equal(error2.Message, errors[1].Message);
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
                Assert.Equal(error1.Message, errors[0].Message);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestEwmaCalculation()
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
            
            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 10);

            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            var etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(0, etlStats.LoadSuccesses);
            Assert.Equal(10, etlStats.TransformationErrors);
            
            Assert.Equal(1.0, etlStats.AverageErrorsRatio.GetRate());
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses >= 50);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 50; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(50, etlStats.LoadSuccesses);
            Assert.Equal(20, etlStats.TransformationErrors);
            
            Assert.InRange(etlStats.AverageErrorsRatio.GetRate(), 0.64, 0.68);
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses >= 950);

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 900; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(950, etlStats.LoadSuccesses);
            Assert.Equal(20, etlStats.TransformationErrors);
            
            Assert.InRange(etlStats.AverageErrorsRatio.GetRate(), 0.05, 0.10);
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 510);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 500; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(950, etlStats.LoadSuccesses);
            Assert.Equal(1020, etlStats.TransformationErrors);
            
            Assert.InRange(etlStats.AverageErrorsRatio.GetRate(), 0.55, 0.58);
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
            const string transformationName3 = "Transformation3";
            const string script3 = """
                                   this.Name = 'Other Company Name';
                                   loadToCompanies(this);
                                   """;
            var collections2 = new List<string>() { "Companies" };
            
            var etlDone1 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName1}" && statistics.LoadErrors >= 5);
            var etlDone2 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName2}/{transformationName2}" && statistics.LoadErrors >= 5);
            var etlDone3 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName2}/{transformationName3}" && statistics.LoadErrors >= 5);
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 5; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe" });
            }
            
            etlDone1.Wait(TimeSpan.FromSeconds(10));
            
            AddEtlTask(src, dest, etlName2, connectionStringName2, [transformationName2, transformationName3], [script2, script3], collections2);
            
            var disableDatabaseResult = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true)).GetAwaiter().GetResult();
            
            Assert.True(disableDatabaseResult.Disabled);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 5; i++)
                    bulkInsert.Store(new Company() { Name = "Some Company" });
            }
            
            etlDone2.Wait(TimeSpan.FromSeconds(10));
            etlDone3.Wait(TimeSpan.FromSeconds(10));

            using (var commands = src.Commands())
            {
                var cmd = new GetEtlTaskErrorsCommand(new List<string>() { etlName1, etlName2 });
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet(nameof(EtlHandlerProcessorForErrors.Response.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<EtlTaskErrors>>(results.ToString());

                var firstTaskErrors = resultsObjectList.Single(x => x.TaskName == $"{etlName1}/{transformationName1}");
                
                Assert.Empty(firstTaskErrors.Errors);
                Assert.Equal(5, firstTaskErrors.PartialErrors.Length);
                
                var secondTaskErrors = resultsObjectList.Single(x => x.TaskName == $"{etlName2}/{transformationName2}");
                
                Assert.Contains(secondTaskErrors.Errors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(secondTaskErrors.PartialErrors);
                
                var thirdTaskErrors = resultsObjectList.Single(x => x.TaskName == $"{etlName2}/{transformationName3}");
                
                Assert.Contains(thirdTaskErrors.Errors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(thirdTaskErrors.PartialErrors);
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestEtlHealthStatusUpdates()
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
            
            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 10);

            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            var etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(0, etlStats.LoadSuccesses);
            Assert.Equal(10, etlStats.TransformationErrors);

            Assert.Equal(EtlTaskHealthStatus.Failed, etlStats.HealthStatus);
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses >= 50);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 50; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(50, etlStats.LoadSuccesses);
            Assert.Equal(20, etlStats.TransformationErrors);

            Assert.Equal(EtlTaskHealthStatus.Impaired, etlStats.HealthStatus);

            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses >= 950);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 900; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(950, etlStats.LoadSuccesses);
            Assert.Equal(20, etlStats.TransformationErrors);

            Assert.Equal(EtlTaskHealthStatus.Healthy, etlStats.HealthStatus);
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 1020);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 1000; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(950, etlStats.LoadSuccesses);
            Assert.Equal(1020, etlStats.TransformationErrors);

            Assert.Equal(EtlTaskHealthStatus.Healthy, etlStats.HealthStatus);
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void InvalidScriptShouldSetTaskHealthToFailed()
    {
        using (var src = GetDocumentStore())
        using (var dest =  GetDocumentStore())
        {
            const string processTag = "Raven ETL";
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   var x = ;

                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };
            
            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 10);
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
                        
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            var etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");

            Assert.Equal(EtlTaskHealthStatus.Failed, etlStats.HealthStatus);
            AssertHealthStatusNotification(src, processTag, $"{etlName1}/{transformationName1}", EtlTaskHealthStatus.Failed);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void EtlTaskHealthStatusChangeNotificationsShouldBeAddedAndRemoved()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string processTag = "Raven ETL";
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
            
            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 10);

            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);

            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            var etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(0, etlStats.LoadSuccesses);
            Assert.Equal(10, etlStats.TransformationErrors);

            AssertHealthStatusNotification(src, processTag, $"{etlName1}/{transformationName1}", EtlTaskHealthStatus.Failed);
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses >= 50);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 50; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(50, etlStats.LoadSuccesses);
            Assert.Equal(20, etlStats.TransformationErrors);
            
            AssertHealthStatusNotification(src, processTag, $"{etlName1}/{transformationName1}", EtlTaskHealthStatus.Disrupted);
            
            etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses >= 1050);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 1000; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));

            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            Assert.Equal(1050, etlStats.LoadSuccesses);
        }
    }
    
    private void AssertHealthStatusNotification(IDocumentStore store, string processTag, string processName, EtlTaskHealthStatus healthStatus)
    {
        var db = GetDatabase(store.Database).GetAwaiter().GetResult();

        var alert = db.NotificationCenter.EtlNotifications.GetAlert<EtlErrorsDetails>(processTag, processName, AlertType.Etl_HealthStatusChange);
        
        Assert.NotNull(alert);
        Assert.Equal($"ETL task health status was changed to {healthStatus}.", alert.Message);
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void AssertTableRecordsAreDeletedOnConfigurationUpdate()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            var collections1 = new List<string>() { "Users" };

            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 10);
            
            var taskId1 = AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            var database = GetDatabase(src.Database).Result;
            using (database.EtlErrorsStorage.ReadPartialErrorsOrderedByCreationDate(out var partialErrorTableValues))
            {
                var partialErrors = partialErrorTableValues.ToList();

                Assert.Equal(20, partialErrors.Count);
            }
            
            var updatedConfig = new RavenEtlConfiguration
            {
                Name = etlName1,
                ConnectionStringName = connectionStringName1,
                MentorNode = null,
                Transforms = [
                    new Transformation()
                    {
                        Name = transformationName1,
                        Collections = collections1,
                        Script = script1,
                        ApplyToAllDocuments = false,
                        Disabled = false
                    }
                ],
                PinToMentorNode = false
            };

            etlDone.Reset();
            
            UpdateRavenEtlTask(src, taskId1, updatedConfig);
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            using (database.EtlErrorsStorage.ReadPartialErrorsOrderedByCreationDate(out var partialErrorTableValues))
            {
                var partialErrors = partialErrorTableValues.ToList();

                Assert.Equal(10, partialErrors.Count);

                foreach (var partialError in partialErrors)
                {
                    Assert.StartsWith($"{etlName1}/{transformationName1}", partialError.Id);
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void AssertTableRecordsAreDeletedOnTaskDeletion()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            const string connectionStringName1 = "ConnectionString1";
            var collections1 = new List<string>() { "Users" };
            
            const string etlName1 = "ETL1";
            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;
            
            const string etlName2 = "ETL2";
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;

            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 10);
            
            var taskId1 = AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            _ = AddEtlTask(src, dest, etlName2, connectionStringName1, [transformationName2], [script2], collections1);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
            
            etlDone.Wait(TimeSpan.FromSeconds(10));
            
            var database = GetDatabase(src.Database).Result;
            using (database.EtlErrorsStorage.ReadPartialErrorsOrderedByCreationDate(out var partialErrorTableValues))
            {
                var partialErrors = partialErrorTableValues.ToList();

                Assert.Equal(20, partialErrors.Count);
            }
            
            var deleteOp = new DeleteOngoingTaskOperation(taskId1, OngoingTaskType.RavenEtl);
            src.Maintenance.Send(deleteOp);
            
            using (database.EtlErrorsStorage.ReadPartialErrorsOrderedByCreationDate(out var partialErrorTableValues))
            {
                var partialErrors = partialErrorTableValues.ToList();

                Assert.Equal(10, partialErrors.Count);

                foreach (var partialError in partialErrors)
                {
                    Assert.StartsWith($"{etlName2}/{transformationName2}", partialError.Id);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void ErrorsLimitInStorageShouldBeRespected()
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
            
            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   if (this.Value == 1)
                                   {
                                        throw new Error("dummy error");
                                   }                   
                                   loadToUsers(this);
                                   """;
            
            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);
            
            var etlDone1 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName1}" && statistics.TransformationErrors >= 700);
            var etlDone2 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName2}" && statistics.TransformationErrors >= 50);
            
            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 650; i++)
                    session.Store(new User { Name = "James Doe", Value = 0 });
                
                for (int i = 0; i < 50; i++)
                    session.Store(new User { Name = "James Doe", Value = 1 });
                
                session.SaveChanges();
            }
            
            etlDone1.Wait(TimeSpan.FromSeconds(15));
            etlDone2.Wait(TimeSpan.FromSeconds(15));
            
            var database = GetDatabase(src.Database).Result;
            using (database.EtlErrorsStorage.ReadPartialErrorsOrderedByCreationDate(out var partialErrorTableValues))
            {
                var partialErrors = partialErrorTableValues.ToList();

                var firstTransformationErrors = partialErrors.Where(x => x.EtlTaskName == $"{etlName1}/{transformationName1}");
                var secondTransformationErrors = partialErrors.Where(x => x.EtlTaskName == $"{etlName1}/{transformationName2}");

                Assert.Equal(100, firstTransformationErrors.Count());
                Assert.Equal(50, secondTransformationErrors.Count());
            }
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void Test()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            //var toggleDatabaseStateResult = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true)).GetAwaiter().GetResult();
            
            //Assert.True(toggleDatabaseStateResult.Disabled);
            
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
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            
            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 5; i++)
                    session.Store(new User { Name = "James Doe", Value = 1 });
                
                session.SaveChanges();
            }
            
            WaitForUserToContinueTheTest(src);
            
            //toggleDatabaseStateResult = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: false)).GetAwaiter().GetResult();
            
            //Assert.False(toggleDatabaseStateResult.Disabled);
            
            WaitForUserToContinueTheTest(src);
            
            var etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");

            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 5; i++)
                    session.Store(new User { Name = "Joe Doe", Value = 1 });
                
                session.SaveChanges();
            }
            
            WaitForUserToContinueTheTest(src);
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            /*
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            WaitForUserToContinueTheTest(src);
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 10; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 1 });
            }
            
            WaitForUserToContinueTheTest(src);
            
            etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
            */
        }
    }

    private EtlProcessStatistics GetEtlStats(IDocumentStore store, string etlName)
    {
        var etl = GetDatabase(store.Database).GetAwaiter().GetResult().EtlLoader.Processes.Single(x => x.Name == etlName);

        return etl.Statistics;
    }

    private static long AddEtlTask(DocumentStore src, DocumentStore dest, string etlName, string connectionStringName, List<string> transformationNames, List<string> transformationScripts, List<string> collections)
    {
        var configuration = new RavenEtlConfiguration
        {
            Name = etlName,
            ConnectionStringName = connectionStringName,
            MentorNode = null,
            Transforms = [],
            PinToMentorNode = false
        };

        foreach ((string transformationName, string transformationScript) in transformationNames.Zip(transformationScripts))
        {
            var transformation = new Transformation
            {
                Name = transformationName,
                Collections = collections,
                Script = transformationScript,
                ApplyToAllDocuments = false,
                Disabled = false
            };
            
            configuration.Transforms.Add(transformation);
        }

        var connectionString = new RavenConnectionString
        {
            Name = connectionStringName,
            Database = dest.Database,
            TopologyDiscoveryUrls = dest.Urls
        };

        src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
        var result = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));
        
        return result.TaskId;
    }

    private static void UpdateRavenEtlTask(IDocumentStore store, long taskId, RavenEtlConfiguration configuration)
    {
        store.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(taskId, configuration));
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
        private readonly List<string> _taskNames;
        
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
