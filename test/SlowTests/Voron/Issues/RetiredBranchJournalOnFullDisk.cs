using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Issues;

// An index holding the last link to a journal it no longer needs donates the file to the shared journals pool. Before the
// donation, CanJournalsBeLinkedWith writes a probe file (<guid>.test-hard-link) into the root's journal directory. On a full disk
// that write throws (ENOSPC), nothing catches it, the sync fails ("The lock task failed") and the index environment ends in
// CatastrophicFailure, so even the disk-full cleanup that should free space kills the index (seen in the Phase III R5 run on Linux).
// The test makes the probe fail without filling a disk: the root's journal directory rejects new files during the index sync.
public class RetiredBranchJournalOnFullDisk(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
    }

    [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Indexes, RavenPlatform.Linux)]
    public async Task IndexSurvivesWhenItsRetiredJournalCannotBeDonated()
    {
        Assert.SkipWhen(Environment.UserName == "root", "root ignores directory permissions");

        using var store = GetDocumentStore(new Options { RunInMemory = false });
        await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition { Name = "Items/ByName", Maps = { "from i in docs.Items select new { i.Name }" } }));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var root = database.IndexStore.SharedJournals.Env;
        var env = database.IndexStore.GetIndex("Items/ByName")._environment;
        root.Options.ManualFlushing = root.Options.ManualSyncing = true;
        env.Options.ManualFlushing = env.Options.ManualSyncing = true;

        // enough index writes to roll the shared journals several times
        for (int batch = 0; batch < 10; batch++)
        {
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 20_000; i++)
                    await bulk.StoreAsync(new Item { Name = new string('x', 200) + i }, $"items/{batch}-{i}");
            }
            Indexes.WaitForIndexing(store);
        }

        // the root lets go of its links, the index now holds the last link to every full journal
        root.FlushLogToDataFile();
        root.SyncDataFileImmediately();
        env.FlushLogToDataFile();

        var indexJournals = env.Options.JournalPath.FullPath;
        var lastLinks = Directory.GetFiles(indexJournals, "*.journal").Where(f => IsHardLink(f) == false).ToList();
        Assert.NotEmpty(lastLinks);

        var rootJournals = root.Options.JournalPath.FullPath;
        var mode = File.GetUnixFileMode(rootJournals);
        File.SetUnixFileMode(rootJournals, UnixFileMode.UserRead | UnixFileMode.UserExecute | UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
                                           UnixFileMode.OtherRead | UnixFileMode.OtherExecute); // like a full disk: no new files there
        try
        {
            var sync = Record.Exception(() => env.SyncDataFileImmediately());

            Assert.Null(sync);
            Assert.False(env.Options.IsCatastrophicFailureSet, "a journal that cannot be donated to the pool must not fail the environment");
            Assert.All(lastLinks, f => Assert.False(File.Exists(f), $"{Path.GetFileName(f)} should be retired (deleted when it cannot be donated)"));
        }
        finally
        {
            File.SetUnixFileMode(rootJournals, mode);
        }
    }

    private static bool IsHardLink(string path)
    {
        Pal.rvn_is_hard_link(path, out var isHardLink, out _);
        return isHardLink;
    }
}
