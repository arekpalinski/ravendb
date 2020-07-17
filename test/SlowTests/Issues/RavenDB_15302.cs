using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15302 : RavenTestBase
    {
        public RavenDB_15302(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Foo()
        {
            if (Directory.Exists(@"C:\workspace\ravendb-4.2_recovery_adi\test\SlowTests\bin\Debug\netcoreapp3.1\Databases"))
                Directory.Delete(@"C:\workspace\ravendb-4.2_recovery_adi\test\SlowTests\bin\Debug\netcoreapp3.1\Databases", true);

            using (var store = GetDocumentStore(new Options()
            {
                RunInMemory = false
            }))
            {
                var restoreConfiguration = new RestoreBackupConfiguration();

                var restoredDbName = "dd" + Guid.NewGuid();

                restoreConfiguration.DatabaseName = restoredDbName;

                var backupPath = @"C:\temp\backups\2020-07-08-12-09-21.ravendb-ff-A-snapshot";
                restoreConfiguration.BackupLocation = backupPath;

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                Operation operation = store.Maintenance.Server.Send(restoreBackupTask);

                operation.WaitForCompletion();

                Task<Operation> importAsync = store.Smuggler.ForDatabase(restoredDbName).ImportAsync(new DatabaseSmugglerImportOptions(), @"C:\workspace\issues\RavenDB-15302\index.ravendbdump");
                importAsync.Wait();

                SpinWait.SpinUntil(() => store.Maintenance.ForDatabase(restoredDbName).Send(new GetStatisticsOperation()).Indexes.Length > 0);


                WaitForIndexing(store, restoredDbName);

                //WaitForUserToContinueTheTest(store, true, restoredDbName);


                var indexDef = store.Maintenance.ForDatabase(restoredDbName).Send(new GetIndexesOperation(0, 10)).First();

                indexDef.Configuration = new IndexConfiguration()
                {
                    { "Indexing.MapBatchSize", "40000" }
                };

                store.Maintenance.ForDatabase(restoredDbName).Send(new PutIndexesOperation(indexDef));

                store.Maintenance.ForDatabase(restoredDbName).Send(new DisableIndexOperation(indexDef.Name));

                store.Operations.ForDatabase(restoredDbName).Send(new PatchByQueryOperation(@"
from UserDocuments update
{
	var groupId = 'urcbgk4scj61d3';

	for (var i = 0; i < this.MemberOfCommunities.length; i++) 
	{
		var memberShip = this.MemberOfCommunities[i];

		memberShip.GroupIds.push(groupId);
	}
}
")).WaitForCompletion();

                ReduceMapResultsOfStaticIndex.ValidatePages = true;

                WaitForUserToContinueTheTest(store, true, restoredDbName);


            }
        }
    }
}
