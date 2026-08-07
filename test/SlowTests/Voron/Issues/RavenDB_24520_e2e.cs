using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

// Server-level investigation of RavenDB-24520 finding F-7: when the shared index-journals ROOT environment
// cannot open, IndexStore.InitializeAsync never reaches OpenIndexesFromRecord, so the whole database fails to
// load rather than one index going faulty.
//
// Two open questions this settles:
//   1. Is the root's initializing transaction actually unsynced in a real database? If it has been synced,
//      recovery skips it via IsAlreadySyncTransaction WITHOUT validating it, and the chain-start guard cannot
//      fire either (it requires LastSyncedTransactionId == -1) - which would make F-7 unreachable in practice.
//   2. Does Indexing.DisableSharedJournals=true rescue a database whose shared root is damaged?
public class RavenDB_24520_e2e(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private static IndexDefinition MapIndex(string name) => new()
    {
        Name = name,
        Maps = { "from i in docs.Items select new { i.Name, i.Value }" }
    };

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task DamagedSharedRoot_IsItReachable_AndDoesDisableSharedJournalsRescueIt()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r =>
            {
                // production default: index open failures produce faulty indexes rather than failing the load
                r.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
            }
        });

        foreach (var n in new[] { "Idx/A", "Idx/B", "Idx/C" })
            await store.Maintenance.SendAsync(new PutIndexesOperation(MapIndex(n)));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.NotNull(database.IndexStore.SharedJournals);

        var rootEnv = database.IndexStore.SharedJournals.Env;
        Guid rootId = rootEnv.HeaderAccessor.JournalId;
        string sharedJournalsDir = Path.Combine(rootEnv.Options.BasePath.FullPath, "Journals");

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 500; i++)
                await bulk.StoreAsync(new Item { Name = $"item-{i}", Value = i }, $"items/{i}");
        }
        Indexes.WaitForIndexing(store);

        // Question 1: what does the root's own sync state look like in a normally-running database?
        var rootJournalInfo = rootEnv.HeaderAccessor.CopyHeader().Journal;
        Output.WriteLine($"root env: JournalId={rootId}");
        Output.WriteLine($"root journal info: LastSyncedJournal={rootJournalInfo.LastSyncedJournal}, " +
                         $"LastSyncedTransactionId={rootJournalInfo.LastSyncedTransactionId}, Flags={rootJournalInfo.Flags}");
        Output.WriteLine($"=> the root's initializing transaction is " +
                         $"{(rootJournalInfo.LastSyncedTransactionId >= 1 ? "ALREADY SYNCED - recovery skips it unvalidated, so F-7 is NOT reachable this way" : "UNSYNCED - F-7 is reachable")}");

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
        await WaitForExclusiveJournalAccessAsync(sharedJournalsDir);

        // corrupt the ROOT's first own transaction in the earliest shared journal
        string journalFile = Directory.GetFiles(sharedJournalsDir, "*.journal").OrderBy(f => f).First();
        List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(await File.ReadAllBytesAsync(journalFile));
        foreach (var group in txs.GroupBy(t => t.JournalId == rootId ? "root" : t.JournalId == WriteAheadJournal.LinkedJournalsRecord.LinkedJournalId ? "linkRecord" : "branch"))
            Output.WriteLine($"{Path.GetFileName(journalFile)} - {group.Key}: {group.Count()} tx(s) at {string.Join(", ", group.Select(t => t.Offset).Take(8))}");

        List<(long Offset, Guid JournalId, long TxId)> rootTxs = txs.Where(t => t.JournalId == rootId).ToList();
        Assert.True(rootTxs.Count > 0, $"the root owns no transaction in {journalFile}");

        var victim = rootTxs[0];
        Output.WriteLine($"corrupting root tx {victim.TxId} at offset {victim.Offset}");
        using (var fs = new FileStream(journalFile, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Position = victim.Offset + TransactionHeader.SizeOf;
            int b = fs.ReadByte();
            fs.Position = victim.Offset + TransactionHeader.SizeOf;
            fs.WriteByte((byte)(b ^ 0xFF));
        }

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));

        string loadFailure = await TryLoadAsync(store);
        Output.WriteLine(loadFailure == null
            ? "database LOADED with the shared root damaged"
            : $"database FAILED to load: {loadFailure}");

        if (loadFailure == null)
        {
            var okStats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
            foreach (var s in okStats)
                Output.WriteLine($"  {s.Name}: State={s.State}, Type={s.Type}, Entries={s.EntriesCount}");
            Output.WriteLine("=> F-7 is not reachable at server level in this configuration; no rescue needed");
            return;
        }

        // Question 2: does turning the feature off let the database come back?
        Output.WriteLine("--- applying Indexing.DisableSharedJournals=true ---");
        await store.Maintenance.SendAsync(new PutDatabaseSettingsOperation(store.Database, new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Indexing.DisableSharedJournals)] = "true"
        }));
        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));

        string rescuedFailure = await TryLoadAsync(store);
        Output.WriteLine(rescuedFailure == null
            ? "database LOADED after disabling shared journals"
            : $"database STILL fails to load: {rescuedFailure}");

        if (rescuedFailure == null)
        {
            long docs;
            using (var session = store.OpenAsyncSession())
                docs = await session.Query<Item>().CountAsync();
            Output.WriteLine($"documents after rescue: {docs}");

            var stats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
            foreach (var s in stats)
                Output.WriteLine($"  {s.Name}: State={s.State}, Type={s.Type}, Entries={s.EntriesCount}");
        }

        Assert.True(rescuedFailure == null,
            $"Indexing.DisableSharedJournals=true did NOT rescue a database whose shared index-journals root is " +
            $"damaged ({rescuedFailure}) - so F-7 leaves an operator with no documented escape hatch, and the error " +
            $"they are shown tells them to create a new database");
    }

    private static async Task<string> TryLoadAsync(Raven.Client.Documents.IDocumentStore store)
    {
        try
        {
            await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());
            return null;
        }
        catch (Exception e)
        {
            return $"{e.GetType().Name}: {FirstLine(e.Message)}";
        }
    }

    private static string FirstLine(string message)
    {
        int idx = message.IndexOf('\n');
        return idx == -1 ? message : message[..idx].TrimEnd('\r');
    }

    private static async Task WaitForExclusiveJournalAccessAsync(string journalsDir)
    {
        foreach (string file in Directory.GetFiles(journalsDir, "*.journal"))
        {
            for (int i = 0; ; i++)
            {
                try
                {
                    using (new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                        break;
                }
                catch (IOException) when (i < 100)
                {
                    await Task.Delay(100);
                }
            }
        }
    }

    private static unsafe List<(long Offset, Guid JournalId, long TxId)> ReadTransactions(byte[] journal)
    {
        var txs = new List<(long, Guid, long)>();
        fixed (byte* p = journal)
        {
            long pos = 0;
            while (pos + TransactionHeader.SizeOf <= journal.Length)
            {
                var header = (TransactionHeader*)(p + pos);
                if (header->HeaderMarker != Constants.TransactionHeaderMarker)
                {
                    pos += 4 * 1024;
                    continue;
                }

                txs.Add((pos, header->JournalId, header->TransactionId));

                long size = header->CompressedSize != -1 ? header->CompressedSize : header->UncompressedSize;
                long sizeIn4Kb = (size + sizeof(TransactionHeader)) / (4 * 1024) +
                                 ((size + sizeof(TransactionHeader)) % (4 * 1024) == 0 ? 0 : 1); // JournalReader.GetTransactionSizeIn4Kb
                pos += sizeIn4Kb * 4 * 1024;
            }
        }

        return txs;
    }
}
