using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Voron.Compaction;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents
{
    public class CompactDatabaseTest : RavenTestBase
    {
        public CompactDatabaseTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabase()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                for (int i = 0; i < 3; i++)
                {
                    await (await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                    }))).WaitForCompletionAsync(TimeSpan.FromSeconds(300));
                }

                await Indexes.WaitForIndexingAsync(store);

                var deleteOperation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));


                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var compactOperation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" }
                }));
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabaseWithAttachment()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var buffer = new byte[16 * 1024 * 1024];
                new Random().NextBytes(buffer);

                using (var session = store.OpenSession())
                using (var fileStream = new MemoryStream(buffer))
                {
                    var user = new User
                    {
                        Name = "Iftah"
                    };
                    session.Store(user, "users/1");
                    session.Advanced.Attachments.Store(user, "randomFile.txt", fileStream);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "randomFile.txt");
                    session.SaveChanges();
                }

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }

        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabaseUsingTempPath()
        {
            var path = NewDataPath();
            var tempPath = NewDataPath(suffix: "-compaction-temp");

            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                // index compaction is not supported for Corax, force Lucene so the custom temp path is exercised for indexes too
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(k => k.Indexing.StaticIndexingEngineType)] = "Lucene"
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                await (await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
                {
                    Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                }))).WaitForCompletionAsync(TimeSpan.FromSeconds(300));

                await Indexes.WaitForIndexingAsync(store);

                var deleteOperation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var compactOperation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" },
                    TempPath = tempPath
                }));
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));
                Assert.True(oldSize > newSize);

                // nothing may be left behind under the compaction temp root
                if (Directory.Exists(tempPath))
                    Assert.Empty(Directory.GetFileSystemEntries(tempPath));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.True(stats.CountOfDocuments > 0);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabaseUsingTempPathAndDeleteOriginalBeforeCopyBack()
        {
            var path = NewDataPath();
            var tempPath = NewDataPath(suffix: "-compaction-temp");

            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                StoreAndDeleteBigAttachment(store);

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    TempPath = tempPath,
                    DeleteOriginalBeforeCopyBack = true
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));
                Assert.True(oldSize > newSize);

                if (Directory.Exists(tempPath))
                    Assert.Empty(Directory.GetFileSystemEntries(tempPath));

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabaseUsingTempPathWhenMoveCrossesDevices()
        {
            var path = NewDataPath();
            var tempPath = NewDataPath(suffix: "-compaction-temp");

            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                StoreAndDeleteBigAttachment(store);

                var database = await GetDatabase(store.Database);
                database.ForTestingPurposesOnly().CompactionForceCrossDeviceMove = true;

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    TempPath = tempPath
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                if (Directory.Exists(tempPath))
                    Assert.Empty(Directory.GetFileSystemEntries(tempPath));

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CompactionRejectsDeleteOriginalBeforeCopyBackWithoutTempPath()
        {
            using (var store = GetDocumentStore(new Options
            {
                Path = NewDataPath()
            }))
            {
                var e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    DeleteOriginalBeforeCopyBack = true
                })));

                Assert.Contains(nameof(CompactSettings.DeleteOriginalBeforeCopyBack), e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabaseUsingCompactionTempPathFromConfiguration()
        {
            var path = NewDataPath();
            var tempPath = NewDataPath(suffix: "-compaction-temp");

            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(k => k.Storage.CompactionTempPath)] = tempPath
            }))
            {
                StoreAndDeleteBigAttachment(store);

                // DeleteOriginalBeforeCopyBack is rejected when no compaction temp path is resolved,
                // so a successful run also proves the configuration entry was picked up
                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    DeleteOriginalBeforeCopyBack = true
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                if (Directory.Exists(tempPath))
                    Assert.Empty(Directory.GetFileSystemEntries(tempPath));

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CompactionKeepsCompactedDataWhenSwapFailsAfterOriginalDataPurge()
        {
            var path = NewDataPath();
            var tempPath = NewDataPath(suffix: "-compaction-temp");

            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                DeleteDatabaseOnDispose = false // the swap is deliberately broken mid-way, the database is not loadable afterwards
            }))
            {
                StoreAndDeleteBigAttachment(store);

                var database = await GetDatabase(store.Database);
                database.ForTestingPurposesOnly().CompactionAfterOriginalDataPurge = () => throw new IOException("injected failure before the compacted data was copied back");

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    TempPath = tempPath,
                    DeleteOriginalBeforeCopyBack = true
                }));

                var e = await Assert.ThrowsAnyAsync<Exception>(() => operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60)));
                Assert.Contains("preserved", e.Message);

                // the compacted data is the only remaining copy - it must survive the failure for manual recovery
                var compactDirectory = Directory.GetDirectories(tempPath).Single(d => d.EndsWith("-compacting", StringComparison.Ordinal));
                Assert.Contains(Directory.GetFiles(compactDirectory), f => Path.GetFileName(f).Equals("Raven.voron", StringComparison.OrdinalIgnoreCase));
            }
        }

        private static void StoreAndDeleteBigAttachment(Raven.Client.Documents.IDocumentStore store)
        {
            var buffer = new byte[16 * 1024 * 1024];
            new Random().NextBytes(buffer);

            using (var session = store.OpenSession())
            using (var fileStream = new MemoryStream(buffer))
            {
                var user = new User
                {
                    Name = "EGR"
                };
                session.Store(user, "users/1");
                session.Advanced.Attachments.Store(user, "randomFile.txt", fileStream);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Attachments.Delete("users/1", "randomFile.txt");
                session.SaveChanges();
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Core, RavenPlatform.Linux)]
        public async Task CanCompactDatabaseWhenDataDirectoryIsSymbolicLink()
        {
            var target = NewDataPath(suffix: "-target", forceCreateDir: true);
            var link = NewDataPath(suffix: "-link");
            Directory.CreateSymbolicLink(link, target);

            using (var store = GetDocumentStore(new Options
            {
                Path = link
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "EGR"
                    }, "users/1");
                    session.SaveChanges();
                }

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var linkInfo = new DirectoryInfo(link);
                Assert.True(linkInfo.LinkTarget != null, "the database directory is no longer a symbolic link after compaction");
                Assert.Equal(new DirectoryInfo(target).FullName, Directory.ResolveLinkTarget(link, returnFinalTarget: true).FullName);

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompactDatabaseWithTxThatSurpassedMaxScratchBufferSize()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(k => k.Storage.MaxScratchBufferSize)] = "1"
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var buffer = new byte[16 * 1024];

                for (int i = 0; i < 32; i++)
                {
                    new Random().NextBytes(buffer);
                    using (var session = store.OpenSession())
                    using (var fileStream = new MemoryStream(buffer))
                    {
                        var user = new User
                        {
                            Name = "EGR"
                        };
                        session.Store(user, "users/1");
                        session.Advanced.Attachments.Store(user, $"randomFile_{i}.txt", fileStream);
                        session.SaveChanges();
                    }
                }

                for (int i = 0; i < 64; i++)
                {
                    using (var session = store.OpenSession())
                    {
                       var u =  session.Load<User>("users/1");
                       u.Age = i;
                       session.SaveChanges();
                    }
                }

                for (int i = 0; i < 32; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.Attachments.Delete("users/1", $"randomFile_{i}.txt");
                        session.SaveChanges();
                    }
                }

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }
        }
    }
}
