using System;
using System.Linq;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;
using Constants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

public class SparseRegionsFreedInSameFlush : StorageTest
{
    private const long ValueSize = 32 * Constants.Size.Megabyte;

    public SparseRegionsFreedInSameFlush(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }

    // idleOnly: true is the Windows default (punched by the idle timer), false is Linux (punched at sync)
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void FreedValueIsPunched(bool idleOnly, bool flushBetween)
    {
        RequireFileBasedPager();
        Options.PunchSparseRegionsOnIdleOnly = idleOnly;

        WriteAndDelete(flushBetween);

        long recorded = Env.CurrentStateRecord.SparseRegions?.Sum(r => r.Count) * Constants.Storage.PageSize ?? 0;
        Assert.True(recorded >= ValueSize * 3 / 4, $"the delete should record the freed run, recorded {new Size(recorded, SizeUnit.Bytes)}");

        Env.FlushLogToDataFile();
        Assert.True(Env.Journal.Applicator.HasPendingSparseRegions, "the freed run should be pending after the flush");

        SyncAndPunch();
        AssertPunched();
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(true)]
    [InlineData(false)]
    public void ReloadPunchesWhatTheFlushMissed(bool idleOnly)
    {
        RequireFileBasedPager();
        Options.PunchSparseRegionsOnIdleOnly = idleOnly;

        WriteAndDelete(flushBetween: false);
        Env.FlushLogToDataFile();
        SyncAndPunch();

        (long allocated, long physical) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        Output.WriteLine($"before reload: allocated={new Size(allocated, SizeUnit.Bytes)}, physical={new Size(physical, SizeUnit.Bytes)}");

        RestartDatabase();
        Env.Options.PunchSparseRegionsOnIdleOnly = idleOnly; // the restart rebuilds the options with the test defaults
        Env.FlushLogToDataFile();
        SyncAndPunch();
        AssertPunched();
    }

    private void WriteAndDelete(bool flushBetween)
    {
        byte[] value = new byte[ValueSize];
        value.AsSpan().Fill(1);

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("t").Add("big", value);
            tx.Commit();
        }

        if (flushBetween)
            Env.FlushLogToDataFile();

        using (var tx = Env.WriteTransaction())
        {
            tx.ReadTree("t").Delete("big");
            tx.Commit();
        }
    }

    private void SyncAndPunch()
    {
        using (var syncOperation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            syncOperation.SyncDataFile();

        if (Env.Options.PunchSparseRegionsOnIdleOnly == false)
            return;

        Env.Options.TimeToPunchSparseRegionsAfterIdle = TimeSpan.Zero;
        while (Env.Journal.Applicator.HasPendingSparseRegions)
            Env.Journal.Applicator.PunchPendingSparseRegionsOnIdle();
    }

    private void AssertPunched()
    {
        (long allocated, long physical) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        Assert.True(physical < allocated - ValueSize * 3 / 4,
            $"Expected the freed value to be punched, but allocated={new Size(allocated, SizeUnit.Bytes)}, physical={new Size(physical, SizeUnit.Bytes)}");
    }
}
