using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using FastTests;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Microsoft.AspNetCore.WebUtilities;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Monitoring;
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
        
        const string etlName2 = "ETL2";
        const string transformationName2 = "Transformation2";

        const string processName1 = $"{etlName1}/{transformationName1}";
        const string processName2 = $"{etlName2}/{transformationName2}";
        
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
            AddEtlTask(src, dest, etlName2, connectionStringName1, [transformationName2], [script1], collections1);

            var now = DateTime.Now;

            var error1 = new EtlProcessError
            {
                CreatedAt = now,
                EtlProcessName = processName1,
                AffectedDocumentsCount = 1,
                Step = EtlErrorStep.Transformation,
                Error = "Test message",
                AdditionalInfo = "some additional info"
            };

            var error2 = new EtlProcessError
            {
                CreatedAt = now.AddDays(1),
                EtlProcessName = processName2,
                AffectedDocumentsCount = 21,
                Step = EtlErrorStep.Load,
                Error = "Test message"
            };
                
            database.EtlErrorsStorage.EnqueueProcessError(error1);
            database.EtlErrorsStorage.EnqueueProcessError(error2);

            using (database.EtlErrorsStorage.ReadAllProcessErrors(out var errorTableValues))
            {
                var errors = errorTableValues.ToList();
                
                Assert.Equal(2, errors.Count);
                
                Assert.Equal(error1.CreatedAt, errors[0].CreatedAt);
                Assert.Equal(error1.EtlProcessName, errors[0].EtlProcessName);
                Assert.Equal(error1.AffectedDocumentsCount, errors[0].AffectedDocumentsCount);
                Assert.Equal((long)error1.Step, errors[0].Step);
                Assert.Equal(error1.Error, errors[0].Error);
                Assert.Equal(error1.AdditionalInfo, errors[0].AdditionalInfo);
                
                Assert.Equal(error2.CreatedAt, errors[1].CreatedAt);
                Assert.Equal(error2.EtlProcessName, errors[1].EtlProcessName);
                Assert.Equal(error2.AffectedDocumentsCount, errors[1].AffectedDocumentsCount);
                Assert.Equal((long)error2.Step, errors[1].Step);
                Assert.Equal(error2.Error, errors[1].Error);
                Assert.Equal(error2.AdditionalInfo, errors[1].AdditionalInfo);
            }
                
            var itemError1 = new EtlItemError
            {
                DocumentId = "doc/1", 
                EtlProcessName = processName1,
                CreatedAt = now,
                Step = EtlErrorStep.Load,
                Error = "Item error"
            };
            
            database.EtlErrorsStorage.EnqueueItemErrors(processName1, [itemError1]);
            
            using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
            {
                var itemErrors = itemErrorTableValues.ToList();

                Assert.Single(itemErrors);
                
                Assert.Equal(itemError1.CreatedAt, itemErrors[0].CreatedAt);
                Assert.Equal(itemError1.EtlProcessName, itemErrors[0].EtlProcessName);
                Assert.Equal(itemError1.DocumentId, itemErrors[0].DocumentId);
                Assert.Equal((long)itemError1.Step, itemErrors[0].Step);
                Assert.Equal(itemError1.Error, itemErrors[0].Error);
                Assert.Equal(itemError1.AdditionalInfo, itemErrors[0].AdditionalInfo);
            }

            using (database.EtlErrorsStorage.ReadProcessErrorsOfEtl(processName1, out var firstTaskErrors))
            {
                var errors = firstTaskErrors.ToList();
                
                Assert.Single(errors);
                
                Assert.Equal(error1.CreatedAt, errors[0].CreatedAt);
                Assert.Equal(error1.EtlProcessName, errors[0].EtlProcessName);
                Assert.Equal(error1.AffectedDocumentsCount, errors[0].AffectedDocumentsCount);
                Assert.Equal((long)error1.Step, errors[0].Step);
                Assert.Equal(error1.Error, errors[0].Error);
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
            
            Assert.InRange(etlStats.AverageErrorsRatio.GetRate(), 0.75, 0.83);
            
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
            
            Assert.InRange(etlStats.AverageErrorsRatio.GetRate(), 0.15, 0.20);
            
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
            
            Assert.InRange(etlStats.AverageErrorsRatio.GetRate(), 0.55, 0.60);
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestGetEtlErrorsEndpoint()
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
                
                res.TryGet(nameof(EtlHandlerProcessorForGetErrors.Response.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<EtlErrors>>(results.ToString());

                var firstTaskErrors = resultsObjectList.Single(x => x.ProcessName == $"{etlName1}/{transformationName1}");
                
                Assert.Empty(firstTaskErrors.ProcessErrors);
                Assert.Equal(5, firstTaskErrors.ItemErrors.Length);
                
                var secondTaskErrors = resultsObjectList.Single(x => x.ProcessName == $"{etlName2}/{transformationName2}");
                
                Assert.Contains(secondTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(secondTaskErrors.ItemErrors);
                
                var thirdTaskErrors = resultsObjectList.Single(x => x.ProcessName == $"{etlName2}/{transformationName3}");
                
                Assert.Contains(thirdTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(thirdTaskErrors.ItemErrors);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void TestGetEtlErrorsEndpointForShardedDatabase(Options options)
    {
        const int shardNumber = 1;
        
        using (var src = GetDocumentStore(options))
        using (var dest = GetDocumentStore(options))
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
                    bulkInsert.Store(new User { Id = $"Users/{i}${shardNumber}", Name = "Joe Doe" });
            }
            
            etlDone1.Wait(TimeSpan.FromSeconds(10));
            
            AddEtlTask(src, dest, etlName2, connectionStringName2, [transformationName2, transformationName3], [script2, script3], collections2);
            
            var disableDatabaseResult = dest.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dest.Database, disable: true)).GetAwaiter().GetResult();
            
            Assert.True(disableDatabaseResult.Disabled);
            
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 5; i++)
                    bulkInsert.Store(new Company() { Id = $"Companies/{i}${shardNumber}", Name = "Some Company" });
            }
            
            etlDone2.Wait(TimeSpan.FromSeconds(10));
            etlDone3.Wait(TimeSpan.FromSeconds(10));

            using (var commands = src.Commands())
            {
                var cmd = new GetEtlTaskErrorsCommand(new List<string>() { etlName1, etlName2 }, isSharded: true, shardNumber);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet(nameof(EtlHandlerProcessorForGetErrors.Response.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<EtlErrors>>(results.ToString());

                var firstTaskErrors = resultsObjectList.Single(x => x.ProcessName == $"{etlName1}/{transformationName1}");
                
                Assert.Empty(firstTaskErrors.ProcessErrors);
                Assert.Equal(5, firstTaskErrors.ItemErrors.Length);
                
                var secondTaskErrors = resultsObjectList.Single(x => x.ProcessName == $"{etlName2}/{transformationName2}");
                
                Assert.Contains(secondTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(secondTaskErrors.ItemErrors);
                
                var thirdTaskErrors = resultsObjectList.Single(x => x.ProcessName == $"{etlName2}/{transformationName3}");
                
                Assert.Contains(thirdTaskErrors.ProcessErrors, x => x.AffectedDocumentsCount == 5);
                Assert.Empty(thirdTaskErrors.ItemErrors);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestDeleteEtlErrorsEndpoint()
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
            
                
            var etlDone1 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName1}" && statistics.LoadErrors >= 5);
                
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
                
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 5; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe" });
            }
                
            etlDone1.Wait(TimeSpan.FromSeconds(10));
            
            using (var commands = src.Commands())
            {
                var deleteErrorsCommand = new DeleteEtlTaskErrorsCommand($"{etlName1}/{transformationName1}");
                commands.Execute(deleteErrorsCommand);

                var getErrorsCommand = new GetEtlTaskErrorsCommand([$"{etlName1}/{transformationName1}"]);
                commands.Execute(getErrorsCommand);
                
                var res = getErrorsCommand.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet(nameof(EtlHandlerProcessorForGetErrors.Response.Results), out BlittableJsonReaderArray results);
                var resultsObjectList = JsonConvert.DeserializeObject<List<EtlErrors>>(results.ToString());
                
                Assert.Single(resultsObjectList);
                Assert.Equal($"{etlName1}/{transformationName1}", resultsObjectList.Single().ProcessName);
                Assert.Empty(resultsObjectList.Single().ProcessErrors);
                Assert.Empty(resultsObjectList.Single().ItemErrors);
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

            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);
            
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

            Assert.Equal(EtlProcessHealthStatus.Impaired, etlStats.HealthStatus);

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

            Assert.Equal(EtlProcessHealthStatus.Impaired, etlStats.HealthStatus);
            
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

            Assert.Equal(EtlProcessHealthStatus.Healthy, etlStats.HealthStatus);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void HealthStatusUpdatesShouldRespectConfiguration()
    {
        var options = new Options()
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusFailedThreshold)] = "0.05";
            }
        };
        
        using (var src = GetDocumentStore(options))
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
                    
            var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.TransformationErrors >= 1);
        
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1], [script1], collections1);
        
            using (var bulkInsert = src.BulkInsert())
            {
                for (int i = 0; i < 9; i++)
                    bulkInsert.Store(new User { Name = "Joe Doe", Value = 0 });
                
                bulkInsert.Store(new User { Name = "James Doe", Value = 0 });
            }
                    
            etlDone.Wait(TimeSpan.FromSeconds(10));
                    
            var etlStats = GetEtlStats(src, $"{etlName1}/{transformationName1}");
        
            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);
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

            Assert.Equal(EtlProcessHealthStatus.Failed, etlStats.HealthStatus);
            AssertHealthStatusNotification(src, processTag, $"{etlName1}/{transformationName1}", EtlProcessHealthStatus.Failed);
        }
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void EtlProcessHealthStatusChangeNotificationsShouldBeAddedAndRemoved()
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

            AssertHealthStatusNotification(src, processTag, $"{etlName1}/{transformationName1}", EtlProcessHealthStatus.Failed);
            
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
            
            AssertHealthStatusNotification(src, processTag, $"{etlName1}/{transformationName1}", EtlProcessHealthStatus.Impaired);
            
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
    
    private void AssertHealthStatusNotification(IDocumentStore store, string processTag, string processName, EtlProcessHealthStatus healthStatus)
    {
        var db = GetDatabase(store.Database).GetAwaiter().GetResult();

        var alert = db.NotificationCenter.EtlNotifications.GetAlert<MessageDetails>(processTag, processName, AlertType.Etl_HealthStatusChange);
        
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
            
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
            {
                var itemErrors = itemErrorTableValues.ToList();

                Assert.Equal(20, itemErrors.Count);
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
            
            etlDone.Wait(TimeSpan.FromSeconds(20));
            
            using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
            {
                var itemErrors = itemErrorTableValues.ToList();

                Assert.Equal(10, itemErrors.Count);

                foreach (var itemError in itemErrors)
                {
                    Assert.StartsWith($"{etlName1}/{transformationName1}", itemError.Id);
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
            
            etlDone.Wait(TimeSpan.FromSeconds(20));
            
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
            {
                var itemErrors = itemErrorTableValues.ToList();

                Assert.Equal(20, itemErrors.Count);
            }
            
            var deleteOp = new DeleteOngoingTaskOperation(taskId1, OngoingTaskType.RavenEtl);
            src.Maintenance.Send(deleteOp);
            
            using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
            {
                var itemErrors = itemErrorTableValues.ToList();

                Assert.Equal(10, itemErrors.Count);

                foreach (var itemError in itemErrors)
                {
                    Assert.StartsWith($"{etlName2}/{transformationName2}", itemError.Id);
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
            using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
            {
                var itemErrors = itemErrorTableValues.ToList();

                var firstTransformationErrors = itemErrors.Where(x => x.EtlProcessName == $"{etlName1}/{transformationName1}");
                var secondTransformationErrors = itemErrors.Where(x => x.EtlProcessName == $"{etlName1}/{transformationName2}");

                Assert.Equal(500, firstTransformationErrors.Count());
                Assert.Equal(50, secondTransformationErrors.Count());
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Etl)]
    public void TestScriptErrorsShouldNotBePersisted()
    {
        using (var store = GetDocumentStore())
        {
            var user = new User() { Id = "users/1", Name = "Joe Doe" };
            
            using (var session = store.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testResult = RavenEtl.TestScript(new TestRavenEtlScript
                {
                    DocumentId = user.Id,
                    Configuration = new RavenEtlConfiguration()
                    {
                        Name = "simulate",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = { "Users" },
                                Name = "Users",
                                Script =
                                    """
                                    throw new Error("dummy error"); 
                                    loadToUsers(this);
                                    """
                            }
                        }
                    }
                }, database, database.ServerStore, context);
                
                var result = (RavenEtlTestScriptResult)testResult;
                            
                Assert.Equal(1, result.ItemTransformationErrors.Count);
                
                using (database.EtlErrorsStorage.ReadAllItemErrors(out var itemErrorTableValues))
                {
                    var itemErrors = itemErrorTableValues.ToList();

                    Assert.Empty(itemErrors);
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Monitoring | RavenTestCategory.Etl)]
    public void CanGetEtlErrorsSnmpMetrics_V2C()
    {
        var port = ReservePort().Port;
        var communityString = "public-test";
        var customSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)] = "true",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.SupportedVersions)] = "V2C",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Port)] = port.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Community)] = communityString
        };

        UseNewLocalServer(customSettings);
        
        using (var src = GetDocumentStore(new Options { CreateDatabase = true }))
        using (var dest = GetDocumentStore(new Options { CreateDatabase = true }))
        {
            var database = GetDatabase(src.Database).GetAwaiter().GetResult();
            
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";

            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe") {
                                       throw new Error("dummy error");
                                   }
                                   
                                   loadToUsers(this);
                                   """;

            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;

            var collections1 = new List<string>() { "Users" };
            
            var etlDone1 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName1}" && statistics.TransformationErrors >= 123);
            var etlDone2 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName2}" && statistics.TransformationErrors >= 123);

            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 123; i++)
                    session.Store(new User { Name = "James Doe", Value = 0 });
                
                session.SaveChanges();
            }
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);
            
            etlDone1.Wait(TimeSpan.FromSeconds(15));
            etlDone2.Wait(TimeSpan.FromSeconds(15));

            var etlDone3 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName1}" && statistics.LoadSuccesses == 4);
            var etlDone4 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName2}" && statistics.TransformationErrors >= 127);
            
            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 4; i++)
                    session.Store(new User { Name = "Joe Doe", Value = 0 });
                
                session.SaveChanges();
            }
            
            etlDone3.Wait(TimeSpan.FromSeconds(15));
            etlDone4.Wait(TimeSpan.FromSeconds(15));
            
            var ip = new Uri(Server.WebUrl).Host;
            var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            
            using (var commands = src.Commands())
            {
                var cmd = new GetSnmpOidsCommand();

                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet("Databases", out BlittableJsonReaderObject databases);
                res.TryGet("Server", out BlittableJsonReaderArray server);
                
                var serverOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(server.ToString());

                var serverEtlErrorsOid = serverOidsObjectList.Single(x => x.Description == "Number of ETL errors").OID;
                
                var result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverEtlErrorsOid))],
                    10000);
                
                Assert.Equal(250, ((Integer32)result.Single().Data).ToInt32());
                
                var serverHealthyEtlsCount = serverOidsObjectList.Single(x => x.Description == $"Number of ETL tasks with {nameof(EtlProcessHealthStatus.Healthy)} health status").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverHealthyEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var serverImpairedEtlsCount = serverOidsObjectList.Single(x => x.Description == $"Number of ETL tasks with {nameof(EtlProcessHealthStatus.Impaired)} health status").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverImpairedEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var serverFailedEtlsCount = serverOidsObjectList.Single(x => x.Description == $"Number of ETL tasks with {nameof(EtlProcessHealthStatus.Failed)} health status").OID;
                                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(serverFailedEtlsCount))],
                    10000);
                                
                Assert.Equal(2, ((Integer32)result.Single().Data).ToInt32());
                
                databases.TryGet(src.Database, out BlittableJsonReaderObject databaseOids);
                
                databaseOids.TryGet("@General", out BlittableJsonReaderArray generalEntries);
                databaseOids.TryGet("Etls", out BlittableJsonReaderObject etlEntries);
                
                var databaseOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(generalEntries.ToString());

                var databaseEtlErrorsOid = databaseOidsObjectList.Single(x => x.Description == "Number of ETL errors").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseEtlErrorsOid))],
                    10000);
                
                Assert.Equal(250, ((Integer32)result.Single().Data).ToInt32());
                
                var databaseHealthyEtlsCount = databaseOidsObjectList.Single(x => x.Description == $"Number of ETL tasks with {nameof(EtlProcessHealthStatus.Healthy)} health status").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseHealthyEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var databaseImpairedEtlsCount = databaseOidsObjectList.Single(x => x.Description == $"Number of ETL tasks with {nameof(EtlProcessHealthStatus.Impaired)} health status").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseImpairedEtlsCount))],
                    10000);
                
                Assert.Equal(0, ((Integer32)result.Single().Data).ToInt32());
                
                var databaseFailedEtlsCount = databaseOidsObjectList.Single(x => x.Description == $"Number of ETL tasks with {nameof(EtlProcessHealthStatus.Failed)} health status").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(databaseFailedEtlsCount))],
                    10000);
                
                Assert.Equal(2, ((Integer32)result.Single().Data).ToInt32());
                
                etlEntries.TryGet($"{etlName1}/{transformationName1}", out BlittableJsonReaderArray firstProcessEntries);
                var firstProcessOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(firstProcessEntries.ToString());
                var firstProcessEtlErrorsOid = firstProcessOidsObjectList.Single(x => x.Description == "Number of task ETL errors").OID;
                var firstProcessHealthStatusOid = firstProcessOidsObjectList.Single(x => x.Description == "Health status of particular ETL task").OID;
                var firstProcessLastSuccessfulBatchTimeOid = firstProcessOidsObjectList.Single(x => x.Description == "Last successful batch time").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessEtlErrorsOid))],
                    10000);
                
                Assert.Equal(123, ((Integer32)result.Single().Data).ToInt32());
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessHealthStatusOid))],
                    10000);
                
                Assert.Equal("Failed", result.Single().Data.ToString());
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(firstProcessLastSuccessfulBatchTimeOid))],
                    10000);
                
                Assert.Equal(SnmpType.TimeTicks, result.Single().Data.TypeCode);
                
                etlEntries.TryGet($"{etlName1}/{transformationName2}", out BlittableJsonReaderArray secondProcessEntries);
                var secondProcessOidsObjectList = JsonConvert.DeserializeObject<List<SnmpEntry>>(secondProcessEntries.ToString());
                var secondProcessEtlErrorsOid = secondProcessOidsObjectList.Single(x => x.Description == "Number of task ETL errors").OID;
                var secondProcessHealthStatusOid = secondProcessOidsObjectList.Single(x => x.Description == "Health status of particular ETL task").OID;
                var secondProcessLastSuccessfulBatchTimeOid = secondProcessOidsObjectList.Single(x => x.Description == "Last successful batch time").OID;
                
                result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier(secondProcessEtlErrorsOid))],
                    10000);
                
               Assert.Equal(127, ((Integer32)result.Single().Data).ToInt32());
               
               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(secondProcessHealthStatusOid))],
                   10000);
               
               Assert.Equal("Failed", result.Single().Data.ToString());
               
               result = Messenger.Get(VersionCode.V2,
                   endpoint,
                   new OctetString(communityString),
                   [new Variable(new ObjectIdentifier(secondProcessLastSuccessfulBatchTimeOid))],
                   10000);
               
               Assert.Equal(SnmpType.Null, result.Single().Data.TypeCode);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Monitoring | RavenTestCategory.Etl)]
    public void EtlsMonitoringEndpointShouldWork()
    {
        using (var src = GetDocumentStore(new Options { CreateDatabase = true }))
        using (var dest = GetDocumentStore(new Options { CreateDatabase = true }))
        {
            const string connectionStringName1 = "ConnectionString1";
            const string etlName1 = "ETL1";

            const string transformationName1 = "Transformation1";
            const string script1 = """
                                   if (this.Name == "James Doe") {
                                       throw new Error("dummy error");
                                   }

                                   loadToUsers(this);
                                   """;

            const string transformationName2 = "Transformation2";
            const string script2 = """
                                   throw new Error("dummy error");
                                   loadToUsers(this);
                                   """;

            var collections1 = new List<string>() { "Users" };
            
            AddEtlTask(src, dest, etlName1, connectionStringName1, [transformationName1, transformationName2], [script1, script2], collections1);

            var etlDone1 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName1}" && statistics.LoadSuccesses == 123);
            var etlDone2 = Etl.WaitForEtlToComplete(src, (name, statistics) => name == $"{etlName1}/{transformationName2}" && statistics.TransformationErrors >= 123);
            
            using (var session = src.OpenSession())
            {
                for (int i = 0; i < 123; i++)
                    session.Store(new User { Name = "Joe Doe", Value = 0 });
                
                session.SaveChanges();
            }
            
            etlDone1.Wait(TimeSpan.FromSeconds(15));
            etlDone2.Wait(TimeSpan.FromSeconds(15));
            
            using (var commands = src.Commands())
            {
                var cmd = new GetEtlsMonitoringDataCommand();

                commands.Execute(cmd);

                var resBjro = cmd.Result as BlittableJsonReaderObject;
                var results = JsonConvert.DeserializeObject<EtlsMetrics>(resBjro.ToString()).Results;
                var databaseResults = results.Single(x => x.DatabaseName == src.Database);
                
                var firstProcessResults = databaseResults.Etls.Single(x => x.ProcessName == $"{etlName1}/{transformationName1}");
                Assert.Equal(EtlProcessHealthStatus.Healthy, firstProcessResults.HealthStatus);
                Assert.NotNull(firstProcessResults.LastSuccessfulBatchTimeInSec);
                Assert.Equal(0, firstProcessResults.ErrorsCount);
                
                var secondProcessResults = databaseResults.Etls.Single(x => x.ProcessName == $"{etlName1}/{transformationName2}");
                Assert.Equal(EtlProcessHealthStatus.Failed, secondProcessResults.HealthStatus);
                Assert.Null(secondProcessResults.LastSuccessfulBatchTimeInSec);
                Assert.Equal(123, secondProcessResults.ErrorsCount);
            }
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
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class SnmpEntry
    {
        public string OID { get; set; }
        public string Description { get; set; }
    }
    
    private class GetEtlTaskErrorsCommand : RavenCommand<object>
    {
        private readonly List<string> _taskNames;
        private readonly bool _isSharded;
        private readonly int _shardNumber;
        
        public GetEtlTaskErrorsCommand(List<string> taskNames, bool isSharded = false, int shardNumber = 0)
        {
            _taskNames = taskNames;
            _isSharded = isSharded;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var baseUrl = $"{node.Url}/databases/{node.Database}/etl/errors";
            
            var queryParams = new Dictionary<string, string>
            {
                { "taskNames", string.Join(',', _taskNames) }
            };

            if (_isSharded)
            {
                queryParams.Add("nodeTag", node.ClusterTag);
                queryParams.Add("shardNumber", _shardNumber.ToString());
            }
            
            url = QueryHelpers.AddQueryString(baseUrl, queryParams);
            
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
    
    private class DeleteEtlTaskErrorsCommand : RavenCommand<object>
    {
        private readonly string _taskName;
        private readonly bool _isSharded;
        private readonly int _shardNumber;
        
        public DeleteEtlTaskErrorsCommand(string taskName, bool isSharded = false, int shardNumber = 0)
        {
            _taskName = taskName;
            _isSharded = isSharded;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var baseUrl = $"{node.Url}/databases/{node.Database}/etl/errors";
            
            var queryParams = new Dictionary<string, string>
            {
                { "name", _taskName }
            };

            if (_isSharded)
            {
                queryParams.Add("nodeTag", node.ClusterTag);
                queryParams.Add("shardNumber", _shardNumber.ToString());
            }
            
            url = QueryHelpers.AddQueryString(baseUrl, queryParams);
            
            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
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
    
    private class GetSnmpOidsCommand : RavenCommand<object>
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/monitoring/snmp/oids";
            
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

    private class GetEtlsMonitoringDataCommand : RavenCommand<object>
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/monitoring/v1/etls";
            
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
