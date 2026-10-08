using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Constants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

// A page that comes back from a punched (sparse) region needs new disk blocks when it is written again. On a full disk that
// io_uring write completes with -ENOSPC, but the PAL worker (src/Raven.Pal/src/posix/ioring.c) sets only the error flag for a
// negative cqe->res and leaves the result at 0, and rvn_write_io_ring reports a failure only through a non-zero error code. The
// flush therefore treats the lost write as done, the sync retires the journal that held the page, and after a restart the
// page reads as zeros (seen in the Phase III R5 run on Linux: "When reading page 2847, we read a page with header of page 0").
//
// The write mode is process-wide: pick it with RAVEN_Storage_WriteMode (Auto, IoRing, FileIo, VectoredFileIo, Mmap).
// Results on Linux 6.8: IoRing (and Auto) fail - the value reads as zeros; FileIo and VectoredFileIo pass - the flush hits
// disk full, keeps the journal and the reopen replays it; Mmap crashes the test process with SIGBUS (exit code 135) when the
// mapped page in the punched region cannot get a block, so run Mmap with the fillDisk: True case on its own.
// Needs a full disk without filling the real one: run inside a user namespace with a small tmpfs, the data file goes there,
// the journals and temp files stay on the regular disk:
//   unshare -Urm sh -c 'mkdir -p /tmp/rvn-small && mount -t tmpfs -o size=64m tmpfs /tmp/rvn-small &&
//     RAVEN_TEST_SMALL_TMPFS=/tmp/rvn-small dotnet test test/SlowTests -c Release --filter "FullyQualifiedName~WriteIntoPunchedRegionOnFullDisk"'
public class WriteIntoPunchedRegionOnFullDisk(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int ValueSize = 4 * Constants.Size.Megabyte;

    // fillDisk: false is the control - the same steps with free space pass
    [RavenMultiplatformTheory(RavenTestCategory.Voron, RavenPlatform.Linux)]
    [InlineData(true)]
    [InlineData(false)]
    public void ValueWrittenIntoAPunchedRegionSurvivesARestart(bool fillDisk)
    {
        var smallDisk = Environment.GetEnvironmentVariable("RAVEN_TEST_SMALL_TMPFS");
        Assert.SkipWhen(string.IsNullOrEmpty(smallDisk), "set RAVEN_TEST_SMALL_TMPFS to a small tmpfs mount (see the comment on the class)");

        var dataPath = Path.Combine(smallDisk, Guid.NewGuid().ToString("N"));
        var otherPath = NewDataPath();
        var fillers = new List<string>();
        var expected = Enumerable.Range(0, ValueSize).Select(i => (byte)(i * 31 + 7)).ToArray();

        try
        {
            using (var options = CreateOptions(dataPath, otherPath))
            using (var env = new StorageEnvironment(options))
            {
                Output.WriteLine($"write mode: {PalConfiguration.WriteMode}");

                Put(env, "first", new byte[ValueSize]);
                FlushAndSync(env);

                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("t").Delete("first");
                    tx.Commit();
                }
                FlushAndSync(env); // Linux punches the freed pages at this sync

                var punched = PhysicalSize(env);
                Output.WriteLine($"data file physical size after the punch: {punched:N0}");

                if (fillDisk)
                    FillDisk(smallDisk, fillers);

                Put(env, "second", expected); // reuses the punched pages
                var flush = Record.Exception(env.FlushLogToDataFile);
                Output.WriteLine($"flush into the full disk: {flush?.GetType().Name ?? "no exception"}, physical size {PhysicalSize(env):N0}");
                Record.Exception(() => env.SyncDataFileImmediately());
            }

            foreach (var filler in fillers)
                File.Delete(filler);

            using (var options = CreateOptions(dataPath, otherPath))
            using (var env = new StorageEnvironment(options))
            using (var tx = env.ReadTransaction())
            {
                var read = tx.ReadTree("t").Read("second");
                Assert.NotNull(read);
                var actual = read.Reader.AsSpan().ToArray();
                Assert.True(actual.AsSpan().SequenceEqual(expected),
                    $"the value written into the punched region came back different: {actual.Count(b => b == 0):N0} of {ValueSize:N0} bytes are zero");
            }
        }
        finally
        {
            foreach (var filler in fillers)
                File.Delete(filler);
            Directory.Delete(dataPath, recursive: true);
        }
    }

    private static StorageEnvironmentOptions CreateOptions(string dataPath, string otherPath)
    {
        var options = StorageEnvironmentOptions.ForPath(dataPath, Path.Combine(otherPath, "Temp"), Path.Combine(otherPath, "Journals"), null, null, null, null);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.PunchSparseRegionsOnIdleOnly = false;
        return options;
    }

    private static void Put(StorageEnvironment env, string key, byte[] value)
    {
        using var tx = env.WriteTransaction();
        tx.CreateTree("t").Add(key, value);
        tx.Commit();
    }

    private static void FlushAndSync(StorageEnvironment env)
    {
        env.FlushLogToDataFile();
        env.SyncDataFileImmediately();
    }

    private static long PhysicalSize(StorageEnvironment env) => env.DataPager.GetFileSize(env.CurrentStateRecord.DataPagerState).PhysicalSize;

    // takes every free block of the small disk, big chunks first, then single pages
    private static void FillDisk(string dir, List<string> fillers)
    {
        foreach (var chunk in new long[] { Constants.Size.Megabyte, 4 * Constants.Size.Kilobyte })
        {
            while (true)
            {
                var path = Path.Combine(dir, $"filler-{fillers.Count}");
                try
                {
                    using (File.OpenHandle(path, FileMode.CreateNew, FileAccess.Write, preallocationSize: chunk))
                    {
                    }
                    fillers.Add(path);
                }
                catch (IOException)
                {
                    File.Delete(path);
                    break;
                }
            }
        }
    }
}
