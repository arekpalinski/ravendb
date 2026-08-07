using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.SharedJournal;

// Corruption scenarios found while testing shared journals under RavenDB-24520 that are NOT covered by the
// RavenDB-27156 / 27166 / 27278 regression tests. See test/RunBooks/RavenDB-24520/findings.
public class RavenDB_24520(ITestOutputHelper output) : RavenTestBase(output)
{
    private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer(32);

    // The LinkedJournalsRecord re-creates branch hard links that a machine-level failure dropped. It carries
    // the sentinel LinkedJournalId and JournalReader.TryReadAndValidateHeader handles it with a `continue`
    // BEFORE VerifyTransactionSequence, so it belongs to no environment's transaction id chain and no sequence
    // check protects it. Since RavenDB-27278 an invalid transaction no longer stops recovery - the 4KB resync
    // scan bypasses it - so a corrupted link record means the repair is skipped.
    //
    // This characterizes the outcome, which is SAFE: the initial validation failure is still reported through
    // OnRecoveryError (only the forward scan suppresses the handlers), and the branch whose link was not
    // restored then fails loudly with InvalidJournalException("No such journal") instead of opening without
    // its data. The gap is diagnostic only - neither message connects the two.
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void BypassedLinkRecordSkipsHardLinkRepairButFailsLoudly(bool corruptLinkRecord)
    {
        var setup = PrepareSharedJournalWithTrailingVictim(encrypted: false);

        List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(File.ReadAllBytes(setup.JournalFile));
        List<(long Offset, Guid JournalId, long TxId)> linkRecords = txs
            .Where(t => t.JournalId == WriteAheadJournal.LinkedJournalsRecord.LinkedJournalId)
            .ToList();
        Assert.True(linkRecords.Count > 0, "the shared journal contains no LinkedJournalsRecord - the fixture is not exercising the repair path");
        Output.WriteLine($"link records at offsets: {string.Join(", ", linkRecords.Select(r => r.Offset))}");

        if (corruptLinkRecord)
        {
            // flip one payload byte of every link record: headers stay readable, only the hash check fails,
            // so the resync bypasses them - the same effect a destroyed region covering them would have.
            // Done before dropping the link because it is the same inode, so the edit survives via the root's link
            using var fs = new FileStream(setup.JournalFile, FileMode.Open, FileAccess.ReadWrite);
            foreach (var record in linkRecords)
            {
                fs.Position = record.Offset + TransactionHeader.SizeOf;
                int b = fs.ReadByte();
                fs.Position = record.Offset + TransactionHeader.SizeOf;
                fs.WriteByte((byte)(b ^ 0xFF));
            }
        }

        // the machine-level failure the link record exists to repair: branch B's hard link is gone
        string branchBJournals = Path.Combine(setup.BranchBPath, "Journals");
        string branchBLink = Directory.GetFiles(branchBJournals).Single();
        string linkName = Path.GetFileName(branchBLink);
        File.Delete(branchBLink);
        Assert.Empty(Directory.GetFiles(branchBJournals));

        var recoveryErrors = new List<string>();
        using var rootOptions = CreateOptions(setup.RootPath, encrypted: false);
        rootOptions.OnRecoveryError += (_, e) => recoveryErrors.Add(e.Message);
        rootOptions.OnIntegrityErrorOfAlreadySyncedData += (_, e) => recoveryErrors.Add(e.Message);

        using var root = new StorageEnvironment(rootOptions);
        using var _ = root.Journal.SharedJournalsScope();

        using (var rootTx = root.ReadTransaction())
            Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());

        bool restored = File.Exists(Path.Combine(branchBJournals, linkName));
        Output.WriteLine($"branch B hard link restored: {restored}; recovery errors reported: {recoveryErrors.Count}");
        foreach (string message in recoveryErrors)
            Output.WriteLine($"  - {message}");

        if (corruptLinkRecord == false)
        {
            // baseline: this is the whole point of the record
            Assert.True(restored, "the root did not re-create branch B's hard link from an intact LinkedJournalsRecord - " +
                                  "the fixture does not reproduce the repair path, so the corrupted case below proves nothing");
            return;
        }

        Assert.False(restored, "expected the corrupted link record to be bypassed - if it was processed, this scenario changed");

        // the consequence: branch B now has to recover with no journal to replay. Nothing was ever flushed or
        // synced, so everything B committed lives only in that journal
        string bValue = null;
        Exception branchBFailure = null;
        try
        {
            using var branchB = OpenBranch(setup.BranchBPath, root, encrypted: false);
            using var tx = branchB.ReadTransaction();
            bValue = tx.ReadTree("treeB")?.Read("b1")?.Reader.ToString();
        }
        catch (Exception e)
        {
            branchBFailure = e;
        }

        Output.WriteLine($"after the skipped repair - branch B opened: {branchBFailure == null}, b1 reads back as: {bValue ?? "<null>"}");
        if (branchBFailure != null)
            Output.WriteLine($"  branch B failure: {branchBFailure.GetType().Name}: {branchBFailure.Message}");

        Assert.True(branchBFailure != null || bValue == "1",
            "the corrupted LinkedJournalsRecord was bypassed by the 4KB resync, so branch B's hard link was never re-created; " +
            $"branch B then opened WITHOUT its journal and silently lost its committed data (b1 reads back as '{bValue ?? "<null>"}'). " +
            "The link record is covered by no transaction-id sequence check, and the reported recovery error says only " +
            "'invalid hash signature' - nothing states that hard-link repair was skipped or what the consequence is");
    }

    // RavenDB-27278 confined the blast radius of a corrupted transaction to its owning environment, but the
    // ROOT of a shared journal is not just another sibling: RavenDB_27278_e2e deliberately picks a victim with
    // no later ROOT transaction because "the root failing to open fails the WHOLE database load, not just
    // indexes". This measures what is left of that hole - how many transactions the root actually owns, and
    // what happens when one of them is the corrupted one.
    [RavenFact(RavenTestCategory.Voron)]
    public void CorruptedRootTransactionStillFailsTheWholeSharedJournalRoot()
    {
        var setup = PrepareSharedJournalWithTrailingVictim(encrypted: false);

        List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(File.ReadAllBytes(setup.JournalFile));
        var byOwner = txs.GroupBy(t => t.JournalId == setup.RootId ? "root"
                : t.JournalId == setup.AId ? "branchA"
                : t.JournalId == setup.BId ? "branchB"
                : t.JournalId == WriteAheadJournal.LinkedJournalsRecord.LinkedJournalId ? "linkRecord"
                : "other");
        foreach (var group in byOwner)
            Output.WriteLine($"{group.Key}: {group.Count()} tx(s) at {string.Join(", ", group.Select(t => t.Offset))}");

        List<(long Offset, Guid JournalId, long TxId)> rootTxs = txs.Where(t => t.JournalId == setup.RootId).ToList();
        Assert.True(rootTxs.Count > 0, "the root owns no transactions in the shared journal - nothing to corrupt");

        // corrupt the root's FIRST transaction, with later root work after it so it cannot truncate away
        var victim = rootTxs[0];
        Assert.Contains(txs, t => t.Offset > victim.Offset);
        Output.WriteLine($"corrupting ROOT tx {victim.TxId} at offset {victim.Offset} ({rootTxs.Count} root tx(s) total)");

        using (var fs = new FileStream(setup.JournalFile, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Position = victim.Offset + TransactionHeader.SizeOf;
            int b = fs.ReadByte();
            fs.Position = victim.Offset + TransactionHeader.SizeOf;
            fs.WriteByte((byte)(b ^ 0xFF));
        }

        using var rootOptions = CreateOptions(setup.RootPath, encrypted: false);
        Exception rootFailure = null;
        try
        {
            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();
            using var tx = root.ReadTransaction();
            Output.WriteLine($"root opened; rootTree/root = {tx.ReadTree("rootTree")?.Read("root")?.Reader.ToString() ?? "<null>"}");
        }
        catch (Exception e)
        {
            rootFailure = e;
            Output.WriteLine($"root FAILED to open: {e.GetType().Name}: {e.Message}");
        }

        // Documents the residual hole rather than asserting it away: in the server the shared-journal root is
        // opened by IndexStore, so a root that cannot open takes down indexing for the whole database, not one
        // index. If this ever starts passing, the root gained the same isolation the branches got in 27278.
        Assert.True(rootFailure != null,
            "the root survived corruption of its own transaction - the residual blast radius documented in F-7 is gone, update the finding");
    }

    // On an ENCRYPTED journal, TryValidateTransaction allocates a 4KB-aligned buffer, copies the transaction
    // into it and appends it to _encryptionBuffers BEFORE attempting decryption; those buffers are freed only
    // in Complete(). Since RavenDB-27278 an invalid transaction triggers TryFindNextValidTransaction, which
    // probes every 4KB boundary to the end of the file.
    //
    // That combination LOOKS like one retained allocation per probed boundary, but it is not: the allocation
    // sits after the HeaderMarker and bounds checks, so a boundary with no transaction header costs nothing.
    // The measured cost of bypassing a foreign corrupted transaction is ~2 buffers (8KB) over ~1,856 probed
    // boundaries. This test pins that property - it fails if the allocation ever moves above the marker check.
    [RavenFact(RavenTestCategory.Voron)]
    public void EncryptedResyncScanMustNotRetainABufferPerProbedBoundary()
    {
        long intact = MeasureRecoveryAllocations(corruptFirstBranchTx: false, out long fileSize, out int txCount);
        long afterResync = MeasureRecoveryAllocations(corruptFirstBranchTx: true, out _, out _);

        long blocks = fileSize / (4 * 1024);
        Output.WriteLine($"journal {fileSize:N0} bytes = {blocks:N0} 4KB blocks, {txCount} transactions");
        Output.WriteLine($"retained native memory during recovery - intact: {intact:N0} bytes, after forced resync: {afterResync:N0} bytes");
        Output.WriteLine($"delta: {afterResync - intact:N0} bytes over {blocks:N0} probed boundaries");

        // The scan should cost bounded memory. Allowing generous headroom over the intact baseline so this
        // pins the leak shape (grows with the scanned region) rather than an exact number.
        long budget = intact + 8 * 1024 * 1024;
        Assert.True(afterResync <= budget,
            $"recovery retained {afterResync:N0} bytes after a forced resync scan versus {intact:N0} intact " +
            $"(+{afterResync - intact:N0}) - TryValidateTransaction keeps an encryption buffer for every probed 4KB " +
            $"boundary until Complete(), so the cost grows with the scanned region and is paid once per environment");
    }

    private long MeasureRecoveryAllocations(bool corruptFirstBranchTx, out long journalSize, out int txCount)
    {
        var setup = PrepareSharedJournalWithTrailingVictim(encrypted: true, extraBranchWrites: 200);
        journalSize = new FileInfo(setup.JournalFile).Length;

        List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(File.ReadAllBytes(setup.JournalFile));
        txCount = txs.Count;

        if (corruptFirstBranchTx)
        {
            // corrupt an early transaction of the OTHER branch, so the measured environment bypasses it and
            // keeps going through the rest of the file. Corrupting its own transaction would abort recovery
            // immediately on the sequence gap and measure nothing.
            var foreign = txs.First(t => t.JournalId == setup.AId);
            Output.WriteLine($"  corrupting foreign (branch A) tx at offset {foreign.Offset} of {journalSize:N0}");
            using var fs = new FileStream(setup.JournalFile, FileMode.Open, FileAccess.ReadWrite);
            fs.Position = foreign.Offset + TransactionHeader.SizeOf;
            int b = fs.ReadByte();
            fs.Position = foreign.Offset + TransactionHeader.SizeOf;
            fs.WriteByte((byte)(b ^ 0xFF));
        }

        using var rootOptions = CreateOptions(setup.RootPath, encrypted: true);
        using var root = new StorageEnvironment(rootOptions);
        using var _ = root.Journal.SharedJournalsScope();

        // the encryption buffers are released in JournalReader.Complete(), which runs before the open returns,
        // so a before/after delta always reads zero - the peak has to be sampled while recovery is running
        long before = global::Sparrow.Utils.NativeMemory.TotalAllocatedMemory;
        long peak = before;
        using var sampling = new ManualResetEventSlim(false);
        var sampler = new Thread(() =>
        {
            while (sampling.IsSet == false)
            {
                long now = global::Sparrow.Utils.NativeMemory.TotalAllocatedMemory;
                if (now > peak)
                    Interlocked.Exchange(ref peak, now);
                Thread.SpinWait(200);
            }
        }) { IsBackground = true };
        sampler.Start();

        try
        {
            using var branchB = OpenBranch(setup.BranchBPath, root, encrypted: true);
        }
        catch (Exception e)
        {
            Output.WriteLine($"  (branch B open threw {e.GetType().Name} - expected for the corrupted variant)");
        }
        finally
        {
            sampling.Set();
            sampler.Join();
        }

        return peak - before;
    }

    // TransactionHeader.Root (offset 48, a 62-byte TreeRootHeader) is, like JournalId, outside both the payload
    // hash and the AEAD authenticated range of header bytes [0,40) - recovery sets the environment's root tree
    // straight from lastTxHeader->Root. So it looks like a second uncompensated field.
    //
    // It is not. This grafts an older Root from the same environment onto its last transaction (a stale-block
    // shape: structurally valid, so no sanity check objects) and shows the data survives. RootPageNumber is
    // unchanged, and the tree content it points at comes from the payload, which IS integrity protected - so a
    // stale Root rolls back only the metadata counters. Pointing RootPageNumber elsewhere would instead hit
    // page-level validation (plain) or per-page decryption (encrypted).
    //
    // Kept as the negative result that narrows F-5 to JournalId alone.
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void StaleRootInTransactionHeaderIsCompensatedByPageLevelIntegrity(bool encrypted)
    {
        var setup = PrepareSharedJournalWithTrailingVictim(encrypted);

        List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(File.ReadAllBytes(setup.JournalFile));
        List<(long Offset, Guid JournalId, long TxId)> aTxs = txs.Where(t => t.JournalId == setup.AId).ToList();
        Assert.True(aTxs.Count >= 2, "need at least two branch A transactions to graft a stale Root");

        const int rootOffset = 48;   // TransactionHeader.Root
        const int rootSize = 110 - 48; // up to TxMarker
        var bytes = File.ReadAllBytes(setup.JournalFile);

        var last = aTxs[^1];
        Assert.Equal(setup.Victim.Offset, last.Offset); // A's last tx is also the file's last tx

        // Consecutive writes to the same tree leave the ROOT tree header untouched, so grafting one onto the
        // next changes nothing. Take the earliest of A's own transactions whose Root actually differs - for A
        // that is its boot transaction, from before treeA existed.
        var previous = aTxs.Take(aTxs.Count - 1)
            .Cast<(long Offset, Guid JournalId, long TxId)?>()
            .FirstOrDefault(t => RootDiffers(bytes, t.Value.Offset, last.Offset));
        Assert.True(previous != null,
            "every earlier branch A transaction carries a byte-identical Root header, so no graft would test anything");

        Output.WriteLine($"grafting Root of tx {previous.Value.TxId} (offset {previous.Value.Offset}) onto tx {last.TxId} (offset {last.Offset})");

        TreeRootHeader target = ReadRoot(bytes, last.Offset);
        TreeRootHeader stale = ReadRoot(bytes, previous.Value.Offset);
        Output.WriteLine($"  last tx root: page {target.RootPageNumber}, {target.NumberOfEntries} entries, depth {target.Depth}");
        Output.WriteLine($"  stale   root: page {stale.RootPageNumber}, {stale.NumberOfEntries} entries, depth {stale.Depth}");

        Buffer.BlockCopy(bytes, (int)previous.Value.Offset + rootOffset, bytes, (int)last.Offset + rootOffset, rootSize);
        File.WriteAllBytes(setup.JournalFile, bytes);

        using var rootOptions = CreateOptions(setup.RootPath, encrypted);
        using var root = new StorageEnvironment(rootOptions);
        using var _ = root.Journal.SharedJournalsScope();

        string victimValue = null;
        string a1Value = null;
        Exception failure = null;
        try
        {
            using var branchA = OpenBranch(setup.BranchAPath, root, encrypted);
            using var tx = branchA.ReadTransaction();
            Tree treeA = tx.ReadTree("treeA");
            victimValue = treeA?.Read("victim")?.Reader.ToString();
            a1Value = treeA?.Read("a1")?.Reader.ToString();
        }
        catch (Exception e)
        {
            failure = e;
        }

        Output.WriteLine(failure != null
            ? $"branch A failed to open: {failure.GetType().Name}: {failure.Message}"
            : $"branch A opened; treeA/a1 = {a1Value ?? "<null>"}, treeA/victim = {victimValue ?? "<null>"}");

        Assert.True(failure != null || victimValue == "y",
            $"grafting a stale Root onto the last transaction lost committed data (treeA/victim reads back as " +
            $"'{victimValue ?? "<null>"}') - if this ever fires, TransactionHeader.Root has become exploitable the way " +
            $"JournalId is in F-5, and the AEAD authenticated range of header bytes [0,40) needs widening");
    }

    private sealed class Setup
    {
        public string RootPath, BranchAPath, BranchBPath, JournalFile;
        public Guid RootId, AId, BId;
        public long BranchBLastTxId;
        public (long Offset, Guid JournalId, long TxId) Victim;
    }

    // Builds a shared journal whose LAST transaction belongs to branch A and carries exactly the transaction
    // id branch B expects next. Nothing is flushed or synced, so every environment fully replays the file.
    // Layout: root tx, A boot, link record, B boot, a1(A), b1(B), victim(A, last).
    private Setup PrepareSharedJournalWithTrailingVictim(bool encrypted, int extraBranchWrites = 0)
    {
        var setup = new Setup
        {
            RootPath = NewDataPath(suffix: "-root"),
            BranchAPath = NewDataPath(suffix: "-branchA"),
            BranchBPath = NewDataPath(suffix: "-branchB"),
        };
        IOExtensions.DeleteDirectory(setup.RootPath);
        IOExtensions.DeleteDirectory(setup.BranchAPath);
        IOExtensions.DeleteDirectory(setup.BranchBPath);

        {
            using var rootOptions = CreateOptions(setup.RootPath, encrypted);
            // must stay a single physical journal file, so scale with the padding
            rootOptions.InitialLogFileSize = 1024 * 1024 + extraBranchWrites * 32 * 1024;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using (var rootTx = root.WriteTransaction())
            {
                rootTx.CreateTree("rootTree").Add("root", "yes");
                rootTx.Commit();
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                var branchA = OpenBranch(setup.BranchAPath, root, encrypted);
                var branchB = OpenBranch(setup.BranchBPath, root, encrypted);

                // a1 puts A's counter one ahead of B's, so A's next transaction id is the one B expects
                using (var tx = branchA.WriteTransaction())
                {
                    tx.CreateTree("treeA").Add("a1", "x");
                    tx.Commit();
                }

                using (var tx = branchB.WriteTransaction())
                {
                    tx.CreateTree("treeB").Add("b1", "1");
                    tx.Commit();
                }

                // pad the journal so a full-file resync scan has a meaningful number of 4KB boundaries to probe
                for (int i = 0; i < extraBranchWrites; i++)
                {
                    using var pad = branchB.WriteTransaction();
                    pad.CreateTree("treeB").Add($"pad/{i}", new string('x', 512));
                    pad.Commit();
                }

                // the victim - last transaction in the file, so nothing after it can trip B's sequence check
                using (var tx = branchA.WriteTransaction())
                {
                    tx.CreateTree("treeA").Add("victim", "y");
                    tx.Commit();
                }

                return (branchA, branchB);
            });
            task.ContinueWith(_ => mre.Set());
            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            var (branchA, branchB) = task.Result;
            setup.RootId = root.HeaderAccessor.JournalId;
            setup.AId = branchA.HeaderAccessor.JournalId;
            setup.BId = branchB.HeaderAccessor.JournalId;

            branchB.Dispose();
            branchA.Dispose();
        }

        setup.JournalFile = Directory.GetFiles(Path.Combine(setup.BranchBPath, "Journals")).Single();
        List<(long Offset, Guid JournalId, long TxId)> txs = ReadTransactions(File.ReadAllBytes(setup.JournalFile));

        Assert.NotEqual(setup.AId, setup.BId);

        setup.Victim = txs[^1];
        Assert.Equal(setup.AId, setup.Victim.JournalId); // the last transaction must be branch A's

        setup.BranchBLastTxId = txs.Where(t => t.JournalId == setup.BId).Select(t => t.TxId).Max();

        // the point of the layout: A's trailing transaction id is exactly what B expects next, so the
        // impersonated transaction slots into B's chain without a gap. Padding B moves its counter far past
        // A's, so this only holds for the unpadded layout the impersonation test uses
        if (extraBranchWrites == 0)
            Assert.Equal(setup.BranchBLastTxId + 1, setup.Victim.TxId);

        return setup;
    }

    private StorageEnvironment OpenBranch(string branchPath, StorageEnvironment root, bool encrypted)
    {
        StorageEnvironmentOptions options = CreateOptions(branchPath, encrypted);
        options.RootJournal = root.Journal;
        return new StorageEnvironment(options);
    }

    private StorageEnvironmentOptions CreateOptions(string path, bool encrypted)
    {
        StorageEnvironmentOptions options = StorageEnvironmentOptions.ForPathForTests(path);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.OnRecoveryError += (_, _) => { }; // the server always subscribes this
        if (encrypted)
            options.Encryption.MasterKey = _masterKey.ToArray(); // all envs of a database share one key
        return options;
    }

    private static unsafe TreeRootHeader ReadRoot(byte[] journal, long txOffset)
    {
        fixed (byte* p = journal)
            return ((TransactionHeader*)(p + txOffset))->Root;
    }

    private static bool RootDiffers(byte[] journal, long leftTxOffset, long rightTxOffset)
    {
        const int rootOffset = 48;     // TransactionHeader.Root
        const int rootSize = 110 - 48; // up to TxMarker
        for (int i = 0; i < rootSize; i++)
        {
            if (journal[leftTxOffset + rootOffset + i] != journal[rightTxOffset + rootOffset + i])
                return true;
        }

        return false;
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
