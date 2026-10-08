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
using Voron.Util;

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
        public Guid EffectiveJournalId;
        public string Owner;
        public bool HashValid;

        public long PayloadOffset => ByteOffset + TransactionHeader.SizeOf;
        public long PayloadSize => Header.CompressedSize != -1 ? Header.CompressedSize : Header.UncompressedSize;
        public bool IsLinkRecord => Header.Flags == TransactionPersistenceModeFlags.LinkedJournalsRecord;
        public bool IsHeaderRecord => (Header.Flags & TransactionPersistenceModeFlags.JournalHeaderRecord) != 0;

        // an encrypted entry is protected by its MAC, its Hash field holds only the incarnation tag
        public bool IsEncrypted => (Header.Flags & TransactionPersistenceModeFlags.Encrypted) == TransactionPersistenceModeFlags.Encrypted;

        public override string ToString()
        {
            return $"@{ByteOffset,12:N0} ({SizeIn4Kb,4} x4KB) txId={Header.TransactionId,-12} owner={Owner,-35} " +
                   $"payload={PayloadSize,10:N0} delta={Header.DurableTxIdDeltaAtSubmit} hash={(HashValid ? "valid" : "INVALID")}" +
                   $"{(IsLinkRecord ? " [LINK-RECORD]" : "")}";
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
        var incarnation = Guid.Empty; // a pre-8.0 journal has no header record
        var incarnationPending = false; // an encrypted header record keeps the incarnation inside its ciphertext

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

                var entry = new TxEntry { ByteOffset = offset, SizeIn4Kb = size4Kb, Header = *header };
                if (entry.IsHeaderRecord)
                {
                    entry.Owner = "<header-record>";
                    if (entry.IsEncrypted)
                    {
                        incarnationPending = true;
                        entry.HashValid = header->Hash == 0;
                    }
                    else
                    {
                        incarnation = *(Guid*)(basePtr + offset + TransactionHeader.SizeOf);
                        entry.HashValid = hash == header->Hash;
                    }
                }
                else
                {
                    if (incarnationPending && entry.IsEncrypted && TryDeriveIncarnation(header, envs, out var derived))
                    {
                        incarnation = derived;
                        incarnationPending = false;
                    }

                    // entries carry JournalId XOR incarnation, and their hash is XORed with the incarnation tag
                    var tag = TransactionHeader.IncarnationTag(incarnation);
                    entry.EffectiveJournalId = header->JournalId.Xor(incarnation);
                    entry.Owner = ResolveOwner(entry.EffectiveJournalId, envs);
                    entry.HashValid = entry.IsEncrypted ? header->Hash == tag : (hash ^ header->Hash) == tag;
                }

                result.Add(entry);

                pos4Kb += size4Kb;
            }
        }
        return result;
    }

    // an encrypted entry's Hash is IncarnationTag(incarnation) and its JournalId is the owner's id XOR incarnation,
    // so the incarnation is JournalId XOR owner for the one known owner whose tag matches
    private static bool TryDeriveIncarnation(TransactionHeader* header, List<EnvInfo> envs, out Guid incarnation)
    {
        foreach (Guid owner in (envs ?? []).Select(e => e.JournalId).Append(WriteAheadJournal.LinkedJournalsRecord.LinkedJournalId))
        {
            incarnation = header->JournalId.Xor(owner);
            if (TransactionHeader.IncarnationTag(incarnation) == header->Hash)
                return true;
        }

        incarnation = Guid.Empty;
        return false;
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

    // ---------------------------------------------------------------- stats

    // Works on any journals dir (a database's, an index's, @SharedJournals). An entry with delta >= 2 was
    // submitted while an earlier one was still in flight, so its count proves pipelining happened.
    public static void PrintJournalStats(string dir)
    {
        var pipelined = 0;
        var envs = JournalOwners(dir);
        foreach (var file in Directory.GetFiles(dir, "*.journal", SearchOption.AllDirectories).OrderBy(x => x))
        {
            var entries = Parse(file, envs);
            var txs = entries.Where(e => e.IsHeaderRecord == false && e.IsLinkRecord == false).ToList();
            var inFlight = txs.Count(t => t.Header.DurableTxIdDeltaAtSubmit >= 2);
            pipelined += inFlight;
            Console.WriteLine($"[stats] {Path.GetRelativePath(dir, file)}: header record={entries.Any(e => e.IsHeaderRecord)}, " +
                              $"transactions={txs.Count}, invalid hash={txs.Count(t => t.HashValid == false)}, " +
                              $"submitted while an earlier one was in flight={inFlight}, " +
                              $"max delta={(txs.Count == 0 ? 0 : txs.Max(t => t.Header.DurableTxIdDeltaAtSubmit))}");
        }
        Console.WriteLine($"[stats] total submitted while an earlier one was in flight: {pipelined}");
    }

    // the environments that can own entries in dir: every env of its database (an encrypted journal needs its owner to recover the incarnation)
    private static List<EnvInfo> JournalOwners(string dir)
    {
        var root = new DirectoryInfo(Path.GetFullPath(dir));
        for (var d = root; d?.Parent != null; d = d.Parent)
        {
            if (string.Equals(d.Parent.Name, "Databases", StringComparison.OrdinalIgnoreCase))
            {
                root = d;
                break;
            }
        }

        return Directory.GetFiles(root.FullName, "database.metadata", SearchOption.AllDirectories)
            .Select(Path.GetDirectoryName)
            .Select(envDir => new EnvInfo { Name = Path.GetFileName(envDir), BasePath = envDir, JournalId = ReadJournalId(envDir) })
            .ToList();
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
