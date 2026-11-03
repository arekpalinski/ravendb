using System;
using Constants = Voron.Global.Constants;

namespace Voron.Impl.Journal
{
    public sealed unsafe partial class JournalFile
    {
        private void AssertEntryBelongsToTransaction(LowLevelTransaction tx, byte* buffer)
        {
            var header = (TransactionHeader*)buffer;
            if (EntryBelongsTo(tx, header) == false)
                throw new InvalidOperationException($"Journal entry in the compression buffer right before the write {Describe(tx, header)}, buffer: 0x{(long)buffer:X}");
        }

        private void AssertEntryOnDiskBelongsToTransaction(LowLevelTransaction tx, long posBy4Kb)
        {
            var header = stackalloc TransactionHeader[1];
            _journalWriter.Read((byte*)header, sizeof(TransactionHeader), posBy4Kb * 4 * Constants.Size.Kilobyte);

            if (EntryBelongsTo(tx, header) == false)
                throw new InvalidOperationException($"Journal entry on disk right after the write {Describe(tx, header)}");
        }

        private static bool EntryBelongsTo(LowLevelTransaction tx, TransactionHeader* header)
        {
            return header->HeaderMarker == Constants.TransactionHeaderMarker && header->TransactionId == tx.Id && header->Hash == tx.TransactionHeader.Hash;
        }

        private string Describe(LowLevelTransaction tx, TransactionHeader* found)
        {
            return $"belongs to another transaction. Expected tx {tx.Id} (hash {tx.TransactionHeader.Hash}), found: {*found}. " +
                   $"Journal: {JournalWriter.FileName.FullPath}, position (4KB): {_writePosIn4Kb}";
        }
    }
}
