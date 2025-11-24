using System;
using System.IO;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_24327 : StorageTest
{
    public RavenDB_24327(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = 1 * 1024 * 1024;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void SparseRegionsCauseInvalidPageAfterRestart_Simple()
    {
        RequireFileBasedPager();

        // initial data

        long startPageNumber = -1;

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
            {
                var p = tx.LowLevelTransaction.AllocatePage(1, zeroPage: true);

                if (startPageNumber == -1)
                    startPageNumber = p.PageNumber;

                Memory.Set(p.DataPointer, 128, Constants.Storage.PageSize - PageHeader.SizeOf);
            }

            tx.Commit();
        }

        // flush

        Env.FlushLogToDataFile();

        // sync

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            var syncResult = operation.SyncDataFile();

            Assert.True(syncResult);
        }

        // modify data to it will write page diffs to journal file (this is important to reproduce the issue)

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
            {
                long pageNumber = startPageNumber + i;
                var p = tx.LowLevelTransaction.ModifyPage(pageNumber);

                Memory.Set(p.DataPointer, 76, Constants.Storage.PageSize - PageHeader.SizeOf);
            }

            tx.Commit();
        }

        // free pages so it will register sparse regions

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 100; i++)
            {
                tx.LowLevelTransaction.FreePage(startPageNumber + i + 10);
            }

            tx.Commit();
        }

        // flush so it will also mark sparse regions in data file

        Env.FlushLogToDataFile();

        RestartDatabase();

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.AllocatePage(1);
            tx.Commit();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void SparseRegionsCauseInvalidPageAfterRestart_MoreFlushes()
    {
        RequireFileBasedPager();

        int numberOfRepeats = 3;

        long[] startPageNumber = new long[numberOfRepeats];

        for (int i = 0; i < numberOfRepeats; i++)
        {
            startPageNumber[i] = -1;
        }

        // initial data

        for (int repeatTx = 0; repeatTx < numberOfRepeats; repeatTx++)
        {
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
                {
                    var p = tx.LowLevelTransaction.AllocatePage(1, zeroPage: true);

                    if (startPageNumber[repeatTx] == -1)
                        startPageNumber[repeatTx] = p.PageNumber;

                    Memory.Set(p.DataPointer, 128, Constants.Storage.PageSize - PageHeader.SizeOf);
                }

                tx.Commit();
            }
        }

        // flush

        Env.FlushLogToDataFile();

        // sync

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            var syncResult = operation.SyncDataFile();

            Assert.True(syncResult);
        }

        // modify data to it will write page diffs to journal file (this is important to reproduce the issue)

        for (int repeatTx = 0; repeatTx < numberOfRepeats; repeatTx++)
        {
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
                {
                    long pageNumber = startPageNumber[repeatTx] + i;
                    var p = tx.LowLevelTransaction.ModifyPage(pageNumber);

                    Memory.Set(p.DataPointer, (byte)((76 % (repeatTx + 1)) + 13), Constants.Storage.PageSize - PageHeader.SizeOf);
                }

                tx.Commit();
            }

            // free pages so it will register sparse regions

            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 100; i++)
                {
                    tx.LowLevelTransaction.FreePage(startPageNumber[repeatTx] + i + 10);
                }

                tx.Commit();
            }

            // flush so it will also mark sparse regions in data file

            Env.FlushLogToDataFile();
        }

        RestartDatabase();

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.AllocatePage(1);
            tx.Commit();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void SparseRegionsCauseInvalidPageAfterRestart_Simple_ButWithSyncs()
    {
        RequireFileBasedPager();

        // initial data

        long startPageNumber = -1;

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
            {
                var p = tx.LowLevelTransaction.AllocatePage(1, zeroPage: true);

                if (startPageNumber == -1)
                    startPageNumber = p.PageNumber;

                Memory.Set(p.DataPointer, 128, Constants.Storage.PageSize - PageHeader.SizeOf);
            }

            tx.Commit();
        }

        // flush

        Env.FlushLogToDataFile();

        // sync

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            var syncResult = operation.SyncDataFile();

            Assert.True(syncResult);
        }

        // modify data to it will write page diffs to journal file (this is important to reproduce the issue)

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
            {
                long pageNumber = startPageNumber + i;
                var p = tx.LowLevelTransaction.ModifyPage(pageNumber);

                Memory.Set(p.DataPointer, 76, Constants.Storage.PageSize - PageHeader.SizeOf);
            }

            tx.Commit();
        }

        // free pages so it will register a sparse region

        long firstFreedPage = -1;
        
        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 100; i++)
            {
                var pageNumber = startPageNumber + i + 10;

                if (firstFreedPage == -1)
                    firstFreedPage = pageNumber;
                
                tx.LowLevelTransaction.FreePage(pageNumber);
            }

            tx.Commit();
        }

        var modifiedPage = -1L;

        using (var tx = Env.WriteTransaction())
        {
            var p = tx.LowLevelTransaction.AllocatePage(10); // this will allocate pages from the sparse region 
            
            Assert.True(p.PageNumber >= firstFreedPage); // so let's assert that
            
            Memory.Set(p.DataPointer, 13, 10 * Constants.Storage.PageSize - PageHeader.SizeOf);

            modifiedPage = p.PageNumber;

            tx.Commit(); // this will it write the entire page to the journal file - no diffing
        }

        Env.FlushLogToDataFile(); // this will create the sparse region in the data file

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            var syncResult = operation.SyncDataFile();

            Assert.True(syncResult);
        }

        // let's modify the page again
        
        using (var tx = Env.WriteTransaction())
        {
            var p = tx.LowLevelTransaction.ModifyPage(modifiedPage);

            Memory.Set(p.DataPointer, 26, 10 * Constants.Storage.PageSize - PageHeader.SizeOf);

            tx.Commit();
        }

        using (var readTx = Env.ReadTransaction())
        {
            // flush under the read tx to prevent flushing and writing the just modified page to the data file (so we'll read just zeros on the next restart)
            
            Env.FlushLogToDataFile();
        }

        RestartDatabase();

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.AllocatePage(1);
            tx.Commit();
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void SparseRegionsCauseInvalidPageAfterRestart_AbortedFlush()
    {
        RequireFileBasedPager();

        // initial data

        long startPageNumber = -1;

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
            {
                var p = tx.LowLevelTransaction.AllocatePage(1, zeroPage: true);

                if (startPageNumber == -1)
                    startPageNumber = p.PageNumber;

                Memory.Set(p.DataPointer, 128, Constants.Storage.PageSize - PageHeader.SizeOf);
            }

            tx.Commit();
        }

        // flush

        Env.FlushLogToDataFile();

        // sync

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            var syncResult = operation.SyncDataFile();

            Assert.True(syncResult);
        }

        // let's modify data to it will write page diffs to a journal file (this is important to reproduce the issue)

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 200; i++)
            {
                long pageNumber = startPageNumber + i;
                var p = tx.LowLevelTransaction.ModifyPage(pageNumber);

                Memory.Set(p.DataPointer, 76, Constants.Storage.PageSize - PageHeader.SizeOf);
            }

            tx.Commit();
        }

        // free pages so it will register sparse regions

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration + 100; i++)
            {
                tx.LowLevelTransaction.FreePage(startPageNumber + i + 10);
            }

            tx.Commit();
        }

        // flush so it will mark sparse regions in the data file BUT also error before writing actual data

        Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFile_AfterSparseRegionsSet_BeforeWritingToDataFile += () =>
        {
            throw new IOException("Simulating error on flush");
        };
        
        Assert.Throws<IOException>(() => Env.FlushLogToDataFile());

        RestartDatabase();

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.AllocatePage(1);
            tx.Commit();
        }
    }
}
