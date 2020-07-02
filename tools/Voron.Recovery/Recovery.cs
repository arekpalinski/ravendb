using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Impl.Paging;
using Voron.Recovery.Journals;
using static Voron.Data.BTrees.Tree;
using Constants = Voron.Global.Constants;

namespace Voron.Recovery
{
    public unsafe class Recovery : IDisposable
    {
        private readonly ExecutionStatus _status = new ExecutionStatus();

        private readonly string _recoveryDirectory;
        private readonly int _pageSize;
        private AbstractPager Pager => _options.DataPager;
        private const string LogFileName = "recovery.log";

        private readonly int _initialContextSize;
        private readonly int _initialContextLongLivedSize;
        private StorageEnvironmentOptions _options;
        private readonly int _progressIntervalInSec;
        private string _lastRecoveredDocumentKey = "No documents recovered yet";
        private readonly string _datafile;
        private readonly bool _copyOnWrite;

        private Logger _logger;
        private readonly byte[] _masterKey;
        private int _InvalidChecksumWithNoneZeroMac;
        private bool _shouldIgnoreInvalidPagesInARaw;
        private const int MaxNumberOfInvalidChecksumWithNoneZeroMac = 128;

        public Recovery(VoronRecoveryConfiguration config)
        {
            _datafile = config.PathToDataFile;
            _recoveryDirectory = config.RecoverDirectory;
            _pageSize = config.PageSizeInKB * Constants.Size.Kilobyte;
            _initialContextSize = config.InitialContextSizeInMB * Constants.Size.Megabyte;
            _initialContextLongLivedSize = config.InitialContextLongLivedSizeInKB * Constants.Size.Kilobyte;

            _masterKey = config.MasterKey;

            // by default CopyOnWriteMode will be true

            _copyOnWrite = !config.DisableCopyOnWriteMode;
            _config = config;
            _options = CreateOptions();

            _progressIntervalInSec = config.ProgressIntervalInSec;
            if (config.LoggingMode != LogMode.None)
                LoggingSource.Instance.SetupLogMode(config.LoggingMode, Path.Combine(Path.GetDirectoryName(_recoveryDirectory), LogFileName), TimeSpan.FromDays(3), long.MaxValue, false);
            _logger = LoggingSource.Instance.GetLogger<Recovery>("Voron Recovery");
            _shouldIgnoreInvalidPagesInARaw = config.IgnoreInvalidPagesInARow;
        }

        private StorageEnvironmentOptions CreateOptions()
        {
            var result = StorageEnvironmentOptions.ForPath(_config.DataFileDirectory, null, null, null, null);
            result.CopyOnWriteMode = _copyOnWrite;
            result.ManualFlushing = true;
            result.ManualSyncing = true;
            result.IgnoreInvalidJournalErrors = _config.IgnoreInvalidJournalErrors;
            result.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = _config.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;
            result.MasterKey = _masterKey;
            return result;
        }

        private readonly byte[] _streamHashState = new byte[(int)Sodium.crypto_generichash_statebytes()];
        private readonly byte[] _streamHashResult = new byte[(int)Sodium.crypto_generichash_bytes()];

        private const int SizeOfMacInBytes = 16;
        private readonly List<(IntPtr Ptr, int Size)> _attachmentChunks = new List<(IntPtr Ptr, int Size)>();
        private readonly VoronRecoveryConfiguration _config;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetFilePosition(long offset, byte* position)
        {
            return (long)position - offset;
        }

        public RecoveryStatus Execute(TextWriter writer, CancellationToken token)
        {
            StorageEnvironment environment = null;
            TempPagerTransaction tempTx = null;
            try
            {
                if (IsEncrypted)
                {
                    // We need a tx for the encryption pager and we can't dispose of it while reading the page
                    tempTx = new TempPagerTransaction();
                }

                var sw = Stopwatch.StartNew();

                if (_copyOnWrite)
                {
                    var journalsRecovery = new JournalsRecovery(() => _options = CreateOptions(), writer);

                    environment = journalsRecovery.RecoverByLoadingEnvironment(_options);
                }

                byte* mem;

                if (environment == null) // journal recovery failed or copy on write is set to false
                    mem = _options.DataPager.PagerState.MapBase;
                else
                    mem = environment.Options.DataPager.PagerState.MapBase;

                long startOffset = (long)mem;
                var fi = new FileInfo(_datafile);
                var fileSize = fi.Length;

                //making sure eof is page aligned
                var eof = mem + (fileSize / _pageSize) * _pageSize;

                DateTime lastProgressReport = DateTime.MinValue;

                if (Directory.Exists(Path.GetDirectoryName(_recoveryDirectory)) == false)
                    Directory.CreateDirectory(Path.GetDirectoryName(_recoveryDirectory));

                using (var recoveryStorage = new RecoveryStorage(_recoveryDirectory).Initialize())
                using (var context = new JsonOperationContext(_initialContextSize, _initialContextLongLivedSize, 8 * 1024, SharedMultipleUseFlag.None))
                {
                    while (mem < eof && token.IsCancellationRequested == false)
                    {
                        try
                        {
                            ReportProgress();

                            var page = DecryptPageIfNeeded(mem, startOffset, ref tempTx, maybePulseTransaction: true);

                            var pageHeader = (PageHeader*)page;

                            if ((pageHeader->Flags).HasFlag(PageFlags.RawData) == false && pageHeader->Flags.HasFlag(PageFlags.Stream) == false)
                            {
                                // this page is not raw data section move on
                                mem += _pageSize;
                                continue;
                            }

                            if (pageHeader->Flags.HasFlag(PageFlags.Single) &&
                                pageHeader->Flags.HasFlag(PageFlags.Overflow))
                            {
                                var message =
                                    $"Page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)}) has both {nameof(PageFlags.Single)} and {nameof(PageFlags.Overflow)} flag turned";

                                mem = PrintErrorAndAdvanceMem(message, mem);
                                continue;
                            }

                            if (pageHeader->Flags.HasFlag(PageFlags.Overflow))
                            {
                                if (ValidateOverflowPage(pageHeader, eof, startOffset, ref mem) == false)
                                    continue;

                                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);

                                if (pageHeader->Flags.HasFlag(PageFlags.Stream))
                                {
                                    var streamPageHeader = (StreamPageHeader*)pageHeader;
                                    //Skipping stream chunks that are not first, this is not a faulty page so we don't report error
                                    if (streamPageHeader->StreamPageFlags.HasFlag(StreamPageFlags.First) == false)
                                    {
                                        mem += numberOfPages * _pageSize;
                                        continue;
                                    }

                                    int rc;
                                    fixed (byte* hashStatePtr = _streamHashState)
                                    fixed (byte* hashResultPtr = _streamHashResult)
                                    {
                                        long totalSize = 0;
                                        _attachmentChunks.Clear();
                                        rc = Sodium.crypto_generichash_init(hashStatePtr, null, UIntPtr.Zero, (UIntPtr)_streamHashResult.Length);
                                        if (rc != 0)
                                        {
                                            Log($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to initialize Sodium for hash computation will skip this page.", LogMode.Operations);
                                            
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }

                                        // write document header, including size
                                        PageHeader* nextPage = pageHeader;

                                        bool valid = true;
                                        string tag = null;

                                        while (true) // has next
                                        {
                                            streamPageHeader = (StreamPageHeader*)nextPage;
                                            //this is the last page and it contains only stream info + maybe the stream tag
                                            if (streamPageHeader->ChunkSize == 0)
                                            {
                                                ExtractTagFromLastPage(nextPage, streamPageHeader, ref tag);
                                                break;
                                            }

                                            totalSize += streamPageHeader->ChunkSize;
                                            var dataStart = (byte*)nextPage + PageHeader.SizeOf;
                                            _attachmentChunks.Add(((IntPtr)dataStart, (int)streamPageHeader->ChunkSize));
                                            rc = Sodium.crypto_generichash_update(hashStatePtr, dataStart, (ulong)streamPageHeader->ChunkSize);
                                            if (rc != 0)
                                            {
                                                Log($"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute chunk hash, will skip it.", LogMode.Operations);

                                                valid = false;
                                                break;
                                            }

                                            if (streamPageHeader->StreamNextPageNumber == 0)
                                            {
                                                ExtractTagFromLastPage(nextPage, streamPageHeader, ref tag);
                                                break;
                                            }

                                            var nextStreamHeader = (byte*)(streamPageHeader->StreamNextPageNumber * _pageSize) + startOffset;
                                            nextPage = (PageHeader*)DecryptPageIfNeeded(nextStreamHeader, startOffset, ref tempTx, false);

                                            //This is the case that the next page isn't a stream page
                                            if (nextPage->Flags.HasFlag(PageFlags.Stream) == false || nextPage->Flags.HasFlag(PageFlags.Overflow) == false)
                                            {
                                                Log($"page #{nextPage->PageNumber} (offset={(long)nextPage}) was suppose to be a stream chunk but isn't marked as Overflow | Stream", LogMode.Operations);
                                                
                                                valid = false;
                                                break;
                                            }

                                            valid = ValidateOverflowPage(nextPage, eof, startOffset, ref mem);

                                            // we already advance the pointer inside the validation
                                            if (valid == false)
                                            {
                                                break;
                                            }
                                        }

                                        if (valid == false)
                                        {
                                            // The first page was valid so we can skip the entire overflow
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }

                                        rc = Sodium.crypto_generichash_final(hashStatePtr, hashResultPtr, (UIntPtr)_streamHashResult.Length);
                                        if (rc != 0)
                                        {
                                            if (_logger.IsOperationsEnabled)
                                                _logger.Operations(
                                                    $"page #{pageHeader->PageNumber} (offset={(long)pageHeader}) failed to compute attachment hash, will skip it.");
                                            mem += numberOfPages * _pageSize;
                                            continue;
                                        }

                                        var hash = new string(' ', 44);
                                        fixed (char* p = hash)
                                        {
                                            var len = Base64.ConvertToBase64Array(p, hashResultPtr, 0, 32);
                                            Debug.Assert(len == 44);
                                        }

                                        var tmpFile = Path.GetTempFileName();

                                        if (File.Exists(tmpFile))
                                            File.Delete(tmpFile);

                                        try
                                        {
                                            using (var fs = new FileStream(tmpFile, FileMode.Create, FileAccess.ReadWrite))
                                            {
                                                foreach (var chunk in _attachmentChunks)
                                                {
                                                    //TODO: make sure that the chunk size may be > 2GB
                                                    var buffer = new Span<byte>(chunk.Ptr.ToPointer(), chunk.Size);
                                                    fs.Write(buffer);
                                                    fs.Flush();
                                                }

                                                fs.Position = 0;
                                                recoveryStorage.PutAttachment(fs, hash, totalSize);
                                            }

                                            _status.NumberOfAttachmentsRetrieved++;
                                        }
                                        finally
                                        {
                                            File.Delete(tmpFile);
                                        }
                                    }

                                    mem += numberOfPages * _pageSize;
                                }
                                else if (Write((byte*)pageHeader + PageHeader.SizeOf, pageHeader->OverflowSize, recoveryStorage, context, startOffset, ((RawDataOverflowPageHeader*)page)->TableType))
                                {
                                    mem += numberOfPages * _pageSize;
                                }
                                else // write document failed
                                {
                                    mem += _pageSize;
                                }

                                continue;
                            }

                            //We don't have checksum for encrypted pages
                            if (IsEncrypted == false)
                            {
                                ulong checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, 0);

                                if (checksum != pageHeader->Checksum)
                                {
                                    CheckInvalidPagesInARaw(pageHeader, mem);
                                    var message =
                                        $"Invalid checksum for page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    continue;
                                }

                                _shouldIgnoreInvalidPagesInARaw = true;
                            }

                            // small raw data section
                            var rawHeader = (RawDataSmallPageHeader*)page;

                            // small raw data section header
                            if (rawHeader->RawDataFlags.HasFlag(RawDataPageFlags.Header))
                            {
                                mem += _pageSize;
                                continue;
                            }

                            if (rawHeader->NextAllocation > _pageSize)
                            {
                                var message =
                                    $"RawDataSmallPage #{rawHeader->PageNumber} at {GetFilePosition(startOffset, mem)} next allocation is larger than {_pageSize} bytes";
                                mem = PrintErrorAndAdvanceMem(message, mem);
                                continue;
                            }

                            for (var pos = PageHeader.SizeOf; pos < rawHeader->NextAllocation;)
                            {
                                var currMem = page + pos;
                                var entry = (RawDataSection.RawDataEntrySizes*)currMem;
                                //this indicates that the current entry is invalid because it is outside the size of a page
                                if (pos > _pageSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                //Allocated size of entry exceed the bound of the page next allocation
                                if (entry->AllocatedSize + pos + sizeof(RawDataSection.RawDataEntrySizes) >
                                    rawHeader->NextAllocation)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}" +
                                        "the allocated entry exceed the bound of the page next allocation.";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                if (entry->UsedSize > entry->AllocatedSize)
                                {
                                    var message =
                                        $"RawDataSmallPage #{rawHeader->PageNumber} has an invalid entry at {GetFilePosition(startOffset, mem + pos)}" +
                                        "the size of the entry exceed the allocated size";
                                    mem = PrintErrorAndAdvanceMem(message, mem);
                                    //we can't retrieve entries past the invalid entry
                                    break;
                                }

                                pos += entry->AllocatedSize + sizeof(RawDataSection.RawDataEntrySizes);
                                if (entry->AllocatedSize == 0 || entry->UsedSize == -1)
                                    continue;

                                if (Write(currMem + sizeof(RawDataSection.RawDataEntrySizes), entry->UsedSize, recoveryStorage, context, startOffset, ((RawDataSmallPageHeader*)page)->TableType) == false)
                                    break;
                            }

                            mem += _pageSize;

                            void ReportProgress()
                            {
                                var now = DateTime.UtcNow;
                                if ((now - lastProgressReport).TotalSeconds >= _progressIntervalInSec)
                                {
                                    if (lastProgressReport != DateTime.MinValue)
                                    {
                                        writer.WriteLine("Press 'q' to quit the recovery process");
                                    }

                                    lastProgressReport = now;
                                    PrintRecoveryProgress(startOffset, mem, eof, now);
                                }
                            }
                        }
                        catch (InvalidOperationException ioe) when (ioe.Message == EncryptedDatabaseWithoutMasterKeyErrorMessage)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            var message =
                                $"Unexpected exception at position {GetFilePosition(startOffset, mem)}:{Environment.NewLine} {e}";
                            mem = PrintErrorAndAdvanceMem(message, mem);
                        }
                    }

                    recoveryStorage.HandleOrphans();

                    PrintRecoveryProgress(startOffset, mem, eof, DateTime.UtcNow);

                    Log(Environment.NewLine +
                        $"Discovered a total of {_status.NumberOfDocumentsRetrieved:#,#;00} documents within {sw.Elapsed.TotalSeconds::#,#.#;;00} seconds." + Environment.NewLine +
                        $"Discovered a total of {_status.NumberOfRevisionsRetrieved:#,#;00} revisions. " + Environment.NewLine +
                        $"Discovered a total of {_status.NumberOfConflictsRetrieved:#,#;00} conflicts. " + Environment.NewLine +
                        $"Discovered a total of {_status.NumberOfAttachmentsRetrieved:#,#;00} attachments. " + Environment.NewLine +
                        $"Discovered a total of {_status.NumberOfCountersRetrieved:#,#;00} counters. " + Environment.NewLine +
                        $"Discovered a total of {_status.NumberOfFaultedPages::#,#;00} faulted pages.", LogMode.Operations);
                }

                if (token.IsCancellationRequested)
                {
                    Log($"Cancellation requested while recovery was in position {GetFilePosition(startOffset, mem)}", LogMode.Operations);

                    return RecoveryStatus.CancellationRequested;
                }

                return RecoveryStatus.Success;
            }
            finally
            {
                tempTx?.Dispose();
                environment?.Dispose();
                if (_config.LoggingMode != LogMode.None)
                    LoggingSource.Instance.EndLogging();
            }

            void PrintRecoveryProgress(long startOffset, byte* mem, byte* eof, DateTime now)
            {
                var curPos = GetFilePosition(startOffset, mem);
                var eofPos = GetFilePosition(startOffset, eof);
                writer.WriteLine(
                    $"{now:hh:MM:ss}: Recovering page at position {curPos:#,#;;0}/{eofPos:#,#;;0} ({(double)curPos / eofPos:p}) - Last recovered doc is {_lastRecoveredDocumentKey}");
            }
        }

        private void Log(string message, LogMode logMode)
        {
            switch (logMode)
            {
                case LogMode.Operations:
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message);
                    break;
                }
                case LogMode.Information:
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info(message);
                    break;
                }
                case LogMode.None:
                    break;
                default:
                    throw new ArgumentException($"Unknown log mode option: {logMode}");
            }
        }

        private void CheckInvalidPagesInARaw(PageHeader* pageHeader, byte* mem)
        {
            if (_shouldIgnoreInvalidPagesInARaw)
                return;

            if (MacNotZero(pageHeader))
            {
                if (MaxNumberOfInvalidChecksumWithNoneZeroMac <= _InvalidChecksumWithNoneZeroMac++)
                {
                    PrintErrorAndAdvanceMem(EncryptedDatabaseWithoutMasterKeyErrorMessage, mem);
                    throw new InvalidOperationException(EncryptedDatabaseWithoutMasterKeyErrorMessage);
                }
            }
        }

        private const string EncryptedDatabaseWithoutMasterKeyErrorMessage =
            "this is a strong indication that you're recovering an encrypted database and didn't" +
            " provide the encryption key using the  '--MasterKey=<KEY>' command line flag";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MacNotZero(PageHeader* pageHeader)
        {
            byte* zeroes = stackalloc byte[SizeOfMacInBytes];
            return Sparrow.Memory.Compare(zeroes, pageHeader->Mac, SizeOfMacInBytes) != 0;
        }

        private readonly Size _maxTransactionSize = new Size(64, SizeUnit.Megabytes);

        private byte* DecryptPageIfNeeded(byte* mem, long start, ref TempPagerTransaction tx, bool maybePulseTransaction = false)
        {
            if (IsEncrypted == false)
                return mem;

            //We must make sure we can close the transaction since it may hold buffers for memory we still need e.g. attachments chunks.
            if (maybePulseTransaction && tx?.TotalEncryptionBufferSize > _maxTransactionSize)
            {
                tx.Dispose();
                tx = new TempPagerTransaction();
            }

            long pageNumber = (long)((PageHeader*)mem)->PageNumber;
            var res = Pager.AcquirePagePointer(tx, pageNumber);

            return res;
        }

        private static void ExtractTagFromLastPage(PageHeader* nextPage, StreamPageHeader* streamPageHeader, ref string tag)
        {
            var si = (StreamInfo*)((byte*)nextPage + streamPageHeader->ChunkSize + PageHeader.SizeOf);
            var tagSize = si->TagSize;
            if (nextPage->OverflowSize > tagSize + streamPageHeader->ChunkSize + StreamInfo.SizeOf)
            {
                //not sure if we should fail because of missing tag
                return;
            }
            if (tagSize > 0)
            {
                tag = Encodings.Utf8.GetString((byte*)si + StreamInfo.SizeOf, tagSize);
            }
        }

        private bool ValidateOverflowPage(PageHeader* pageHeader, byte* eof, long startOffset, ref byte* mem)
        {
            ulong checksum;
            //pageHeader might be a buffer address we need to verify we don't exceed the original memory boundary here
            var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            var sizeOfPages = numberOfPages * _pageSize;
            var endOfOverflow = (long)mem + sizeOfPages;
            // the endOfOverflow can be equal to eof if the last page is overflow
            if (endOfOverflow > (long)eof)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" size exceeds the end of the file ([{(long)mem}:{(long)endOfOverflow}])";
                mem = PrintErrorAndAdvanceMem(message, mem);
                return false;
            }

            if (pageHeader->OverflowSize <= 0)
            {
                var message =
                    $"Overflow page #{pageHeader->PageNumber} (offset={GetFilePosition(startOffset, mem)})" +
                    $" OverflowSize is not a positive number ({pageHeader->OverflowSize})";
                mem = PrintErrorAndAdvanceMem(message, mem);
                return false;
            }

            if (IsEncrypted == false)
            {
                // this can only be here if we know that the overflow size is valid
                checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);

                if (checksum != pageHeader->Checksum)
                {
                    CheckInvalidPagesInARaw(pageHeader, mem);
                    var message =
                        $"Invalid checksum for overflow page {pageHeader->PageNumber}, expected hash to be {pageHeader->Checksum} but was {checksum}";
                    mem = PrintErrorAndAdvanceMem(message, mem);
                    return false;
                }

                _shouldIgnoreInvalidPagesInARaw = true;
            }

            return true;
        }

        private bool Write(byte* mem, int sizeInBytes, RecoveryStorage recoveryStorage, JsonOperationContext context, long startOffset, byte tableType)
        {
            switch ((TableType)tableType)
            {
                case TableType.None:
                    return false;
                case TableType.Documents:
                    return WriteDocument(mem, sizeInBytes, recoveryStorage, context, startOffset);
                case TableType.Revisions:
                    return WriteRevision(mem, sizeInBytes, recoveryStorage, context, startOffset);
                case TableType.Conflicts:
                    return WriteConflict(mem, sizeInBytes, recoveryStorage, context, startOffset);
                case TableType.Counters:
                    return WriteCounter(mem, sizeInBytes, recoveryStorage, context, startOffset);
                default:
                    throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null);
            }
        }

        private bool WriteCounter(byte* mem, int sizeInBytes, RecoveryStorage recoveryStorage, JsonOperationContext context, long startOffset)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                using (CounterGroupDetail counterGroup = CountersStorage.TableValueToCounterGroupDetail(context, ref tvr))
                {
                    try
                    {
                        if (counterGroup == null)
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Failed to convert table value to counter at position {GetFilePosition(startOffset, mem)}");
                            return false;
                        }

                        CountersStorage.ConvertFromBlobToNumbers(context, counterGroup);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(
                                $"Found invalid counter item at position={GetFilePosition(startOffset, mem)} with document Id={counterGroup?.DocumentId ?? "null"} and counter values={counterGroup?.Values}", e);
                        return false;
                    }

                    recoveryStorage.PutCounter(counterGroup);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Found counter item with document Id={counterGroup.DocumentId} and counter values={counterGroup.Values}");

                    _lastRecoveredDocumentKey = counterGroup.DocumentId;
                    _status.NumberOfCountersRetrieved++;

                    return true;
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing counter item at position {GetFilePosition(startOffset, mem)}: {e}");
                return false;
            }
        }

        private bool WriteDocument(byte* mem, int sizeInBytes, RecoveryStorage recoveryStorage, JsonOperationContext context, long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                using (var document = DocumentsStorage.ParseRawDataSectionDocumentWithValidation(context, ref tvr, sizeInBytes))
                {
                    try
                    {
                        if (document == null)
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Failed to convert table value to document at position {GetFilePosition(startOffest, mem)}");
                            return false;
                        }

                        document.Data.BlittableValidation();

                        var existingDoc = recoveryStorage.Get(document.Id);

                        if (existingDoc != null)
                        {
                            // This is a duplicate doc. It can happen when a page is marked as freed, but still exists in the data file.
                            // We determine which one to choose by their etag. If the document is newer, we will write it again to the
                            // smuggler file. This way, when importing, it will be the one chosen (last write wins)
                            if (document.Etag <= existingDoc.Etag)
                                return false;
                        }
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(
                                $"Found invalid blittable document at pos={GetFilePosition(startOffest, mem)} with key={document?.Id ?? "null"}", e);
                        return false;
                    }
                    
                    recoveryStorage.PutDocument(document);

                    _status.NumberOfDocumentsRetrieved++;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Found document with key={document.Id}");

                    _lastRecoveredDocumentKey = document.Id;

                    return true;
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing document at position {GetFilePosition(startOffest, mem)}: {e}");

                return false;
            }
        }


        private bool WriteRevision(byte* mem, int sizeInBytes, RecoveryStorage recoveryStorage, JsonOperationContext context, long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                using (Document revision = RevisionsStorage.ParseRawDataSectionRevisionWithValidation(context, ref tvr, sizeInBytes, out var changeVector))
                {
                    try
                    {
                        if (revision == null)
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Failed to convert table value to revision document at position {GetFilePosition(startOffest, mem)}");
                            return false;
                        }

                        revision.Data.BlittableValidation();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(
                                $"Found invalid blittable revision document at pos={GetFilePosition(startOffest, mem)} with key={revision?.Id ?? "null"}", e);
                        return false;
                    }

                    recoveryStorage.PutRevision(revision);

                    _status.NumberOfRevisionsRetrieved++;
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Found revision document with key={revision.Id}");
                    _lastRecoveredDocumentKey = revision.Id;
                    return true;
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing revision document at position {GetFilePosition(startOffest, mem)}: {e}");

                return false;
            }
        }

        private bool WriteConflict(byte* mem, int sizeInBytes, RecoveryStorage recoveryStorage, JsonOperationContext context, long startOffest)
        {
            try
            {
                var tvr = new TableValueReader(mem, sizeInBytes);

                using (DocumentConflict conflict = ConflictsStorage.ParseRawDataSectionConflictWithValidation(context, ref tvr, sizeInBytes, out var changeVector))
                {
                    try
                    {
                        if (conflict == null)
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Failed to convert table value to conflict document at position {GetFilePosition(startOffest, mem)}");
                            return false;
                        }

                        conflict.Doc.BlittableValidation();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(
                                $"Found invalid blittable conflict document at pos={GetFilePosition(startOffest, mem)} with key={conflict?.Id ?? "null"}", e);
                        return false;
                    }

                    recoveryStorage.PutConflict(conflict);

                    _status.NumberOfConflictsRetrieved++;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Found conflict document with key={conflict.Id}");
                    _lastRecoveredDocumentKey = conflict.Id;
                    return true;
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected exception while writing conflict document at position {GetFilePosition(startOffest, mem)}: {e}");
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* PrintErrorAndAdvanceMem(string message, byte* mem)
        {
            Log(message, LogMode.Operations);

            _status.NumberOfFaultedPages++;
            return mem + _pageSize;
        }

        public bool IsEncrypted => _masterKey != null;

        public enum RecoveryStatus
        {
            Success,
            CancellationRequested
        }

        private class ByDocIdAndCounterName : IComparer<(string name, string docId)>
        {
            public int Compare((string name, string docId) x, (string name, string docId) y)
            {
                return CaseInsensitiveComparer.Default.Compare(x.docId + SpecialChars.RecordSeparator + x.name,
                    y.docId + SpecialChars.RecordSeparator + y.name);
            }
        }

        public void Dispose()
        {
            _options?.Dispose();
        }
    }
}
