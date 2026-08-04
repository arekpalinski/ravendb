using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Sparrow;
using Voron;
using Voron.Global;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;

namespace Tryouts.RavenDB_24520;

// Offline journal parser + surgical corruptor for shared-journal files.
// Everything operates on files at rest - never run against a live server.
public static unsafe class JournalTools
{
    private const int Block = 4 * 1024;

    public sealed class EnvInfo
    {
        public string Name;
        public string BasePath;
        public Guid JournalId;
        public FileHeader? Header;

        public override string ToString()
        {
            var h = Header;
            var sync = h == null ? "headers: <unreadable>"
                : $"txId={h.Value.TransactionId} lastSyncedJournal={h.Value.Journal.LastSyncedJournal} lastSyncedTx={h.Value.Journal.LastSyncedTransactionId}";
            return $"{Name,-35} journalId={JournalId} {sync}";
        }
    }

    public sealed class TxEntry
    {
        public long ByteOffset;
        public int SizeIn4Kb;
        public TransactionHeader Header;
        public string Owner;
        public bool HashValid;

        public long PayloadOffset => ByteOffset + TransactionHeader.SizeOf;
        public long PayloadSize => Header.CompressedSize != -1 ? Header.CompressedSize : Header.UncompressedSize;
        public bool IsLinkRecord => Header.Flags == TransactionPersistenceModeFlags.LinkedJournalsRecord;

        public override string ToString()
        {
            return $"@{ByteOffset,12:N0} ({SizeIn4Kb,4} x4KB) txId={Header.TransactionId,-12} owner={Owner,-35} " +
                   $"payload={PayloadSize,10:N0} hash={(HashValid ? "valid" : "INVALID")}{(IsLinkRecord ? " [LINK-RECORD]" : "")}";
        }
    }

    // ---------------------------------------------------------------- discovery

    public static List<EnvInfo> DiscoverEnvironments(string dataDir)
    {
        var result = new List<EnvInfo>();
        var indexesDir = Path.Combine(dataDir, "Databases", Harness.DbName, "Indexes");
        if (Directory.Exists(indexesDir) == false)
            return result;

        foreach (var envDir in Directory.GetDirectories(indexesDir).OrderBy(d => Path.GetFileName(d) == "@SharedJournals" ? 0 : 1).ThenBy(x => x))
        {
            var name = Path.GetFileName(envDir);
            result.Add(new EnvInfo { Name = name, BasePath = envDir, JournalId = ReadJournalId(envDir), Header = ReadFileHeader(envDir) });
        }
        return result;
    }

    public static Guid ReadJournalId(string envBasePath)
    {
        var path = Path.Combine(envBasePath, "database.metadata");
        if (File.Exists(path) == false)
            return Guid.Empty;
        var bytes = File.ReadAllBytes(path);
        if (bytes.Length < Marshal.SizeOf<MetadataFile>())
            return Guid.Empty;
        var metadata = MemoryMarshal.Read<MetadataFile>(bytes);
        return metadata.JournalId;
    }

    public static FileHeader? ReadFileHeader(string envBasePath)
    {
        FileHeader? best = null;
        foreach (var name in new[] { "headers.one", "headers.two" })
        {
            var path = Path.Combine(envBasePath, name);
            if (File.Exists(path) == false)
                continue;
            var bytes = File.ReadAllBytes(path);
            if (bytes.Length < sizeof(FileHeader))
                continue;
            var header = MemoryMarshal.Read<FileHeader>(bytes);
            if (header.MagicMarker != Constants.MagicMarker)
                continue;
            if (best == null || header.HeaderRevision > best.Value.HeaderRevision)
                best = header;
        }
        return best;
    }

    // ---------------------------------------------------------------- parsing

    public static List<TxEntry> Parse(string journalPath, List<EnvInfo> envs = null)
    {
        var result = new List<TxEntry>();
        var bytes = File.ReadAllBytes(journalPath);
        var total4Kb = bytes.Length / Block;

        fixed (byte* basePtr = bytes)
        {
            for (long pos4Kb = 0; pos4Kb < total4Kb;)
            {
                var offset = pos4Kb * Block;
                var header = (TransactionHeader*)(basePtr + offset);
                if (header->HeaderMarker != Constants.TransactionHeaderMarker)
                {
                    pos4Kb++;
                    continue;
                }

                var payloadSize = header->CompressedSize != -1 ? header->CompressedSize : header->UncompressedSize;
                var actualSize = TransactionHeader.SizeOf + payloadSize;
                if (payloadSize < 0 || offset + actualSize > bytes.Length)
                {
                    // header claims more data than the file holds - treat like the recovery loop does
                    pos4Kb++;
                    continue;
                }

                var size4Kb = (int)((actualSize - 1) / Block + 1);
                var hash = Hashing.XXHash64.Calculate(basePtr + offset + TransactionHeader.SizeOf, (ulong)payloadSize, (ulong)header->TransactionId);

                result.Add(new TxEntry
                {
                    ByteOffset = offset,
                    SizeIn4Kb = size4Kb,
                    Header = *header,
                    HashValid = hash == header->Hash,
                    Owner = ResolveOwner(header->JournalId, envs)
                });

                pos4Kb += size4Kb;
            }
        }
        return result;
    }

    private static string ResolveOwner(Guid journalId, List<EnvInfo> envs)
    {
        if (journalId == WriteAheadJournal.LinkedJournalsRecord.LinkedJournalId)
            return "<link-record>";
        if (journalId == Guid.Empty)
            return "<legacy>";
        var env = envs?.FirstOrDefault(e => e.JournalId == journalId);
        return env?.Name ?? $"<unknown {journalId}>";
    }

    // ---------------------------------------------------------------- map

    public static void PrintMap(string dataDir)
    {
        var envs = DiscoverEnvironments(dataDir);
        Console.WriteLine($"[map] environments in {dataDir}:");
        foreach (var env in envs)
            Console.WriteLine($"[map]   {env}");

        var allJournals = Harness.SharedJournalFiles(dataDir).Concat(Harness.BranchJournalFiles(dataDir)).ToList();
        var inodeGroups = allJournals.GroupBy(Harness.GetFileId).ToList();

        Console.WriteLine($"[map] {allJournals.Count} journal files, {inodeGroups.Count} distinct inodes:");
        foreach (var group in inodeGroups)
        {
            var files = group.ToList();
            Console.WriteLine($"[map] inode {group.Key} links={Harness.GetHardLinkCount(files[0])}:");
            foreach (var f in files)
                Console.WriteLine($"[map]     {Path.GetRelativePath(dataDir, f)}");
            foreach (var tx in Parse(files[0], envs))
                Console.WriteLine($"[map]       {tx}");
        }
    }

    // ---------------------------------------------------------------- corruption ops

    public static void FlipPayloadBytes(string journalPath, TxEntry tx, long offsetInPayload = 0, int count = 1)
    {
        if (offsetInPayload >= tx.PayloadSize)
            throw new ArgumentOutOfRangeException(nameof(offsetInPayload), $"payload is only {tx.PayloadSize} bytes");
        XorBytes(journalPath, tx.PayloadOffset + offsetInPayload, count);
        Console.WriteLine($"[corrupt] flipped {count} payload byte(s) of tx {tx.Header.TransactionId} (owner {tx.Owner}) at payload+{offsetInPayload} in {Path.GetFileName(journalPath)}");
    }

    public static void SmashHeaderMarker(string journalPath, TxEntry tx)
    {
        XorBytes(journalPath, tx.ByteOffset, 8);
        Console.WriteLine($"[corrupt] smashed header marker of tx {tx.Header.TransactionId} (owner {tx.Owner}) in {Path.GetFileName(journalPath)}");
    }

    public static void CorruptHeaderHash(string journalPath, TxEntry tx)
    {
        XorBytes(journalPath, tx.ByteOffset + 40, 8); // Hash field
        Console.WriteLine($"[corrupt] corrupted Hash of tx {tx.Header.TransactionId} (owner {tx.Owner})");
    }

    public static void CorruptHeaderTxId(string journalPath, TxEntry tx)
    {
        XorBytes(journalPath, tx.ByteOffset + 8, 1); // low byte of TransactionId
        Console.WriteLine($"[corrupt] corrupted TransactionId of tx {tx.Header.TransactionId} (owner {tx.Owner})");
    }

    public static void CorruptHeaderJournalId(string journalPath, TxEntry tx)
    {
        XorBytes(journalPath, tx.ByteOffset + 136, 16); // JournalId guid
        Console.WriteLine($"[corrupt] corrupted JournalId of tx {tx.Header.TransactionId} (owner was {tx.Owner})");
    }

    public static void ZeroBlock(string journalPath, long block4Kb, int blocks = 1)
    {
        using var fs = new FileStream(journalPath, FileMode.Open, FileAccess.Write);
        fs.Position = block4Kb * Block;
        fs.Write(new byte[blocks * Block]);
        Console.WriteLine($"[corrupt] zeroed {blocks} 4KB block(s) at block {block4Kb} of {Path.GetFileName(journalPath)}");
    }

    public static void TruncateAt(string journalPath, long size)
    {
        using var fs = new FileStream(journalPath, FileMode.Open, FileAccess.Write);
        fs.SetLength(size);
        Console.WriteLine($"[corrupt] truncated {Path.GetFileName(journalPath)} to {size:N0} bytes");
    }

    public static void DeleteJournal(string journalPath)
    {
        File.Delete(journalPath);
        Console.WriteLine($"[corrupt] deleted {journalPath}");
    }

    // replaces a hard link with an independent copy of the file content (same name, new inode),
    // then optionally corrupts one byte in the copy - simulates "directory was copied, links broken"
    public static void DivergeCopy(string journalPath, long? flipByteAt = null)
    {
        var tmp = journalPath + ".diverge-tmp";
        File.Copy(journalPath, tmp);
        File.Delete(journalPath);
        File.Move(tmp, journalPath);
        if (flipByteAt.HasValue)
            XorBytes(journalPath, flipByteAt.Value, 1);
        Console.WriteLine($"[corrupt] diverged {journalPath} into its own inode{(flipByteAt.HasValue ? $" + flipped byte at {flipByteAt}" : "")}");
    }

    private static void XorBytes(string path, long offset, int count)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        var buffer = new byte[count];
        fs.Position = offset;
        fs.ReadExactly(buffer);
        for (int i = 0; i < count; i++)
            buffer[i] ^= 0xFF;
        fs.Position = offset;
        fs.Write(buffer);
    }
}
