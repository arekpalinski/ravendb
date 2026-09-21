using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Voron;
using Voron.Global;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// d-flusher-liveness: a hot commit stream on a slow-classified device (every journal write sleeps)
    /// for most of the budget, with the REAL flusher and sync workers. The flusher yield logic must
    /// never starve: the last flushed transaction id has to keep advancing, the journal file count and
    /// the unsynced byte count must stay bounded. When the stream stops, flush and sync must complete
    /// on their own within a bound (the yielded-flush retry on the idle tick).
    /// </summary>
    public static class DFlusherLiveness
    {
        private const int JournalWriteSleepMs = 6;
        private const int MaxStallSeconds = 30;
        private const int MaxJournalFiles = 96;
        private const long MaxUnsyncedBytes = 256L * Constants.Size.Megabyte;
        private const int DrainTimeoutSeconds = 90;

        public static int Run(StressContext ctx)
        {
            ctx.Iterations = 1;
            var dir = ctx.IterationDir(0);
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
            Directory.CreateDirectory(dir);

            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = false;
            options.ManualSyncing = false;
            options.MaxLogFileSize = Constants.Size.Megabyte;
            options.MaxNumberOfPagesInJournalBeforeFlush = 128;
            options.MaxUnsyncedBytesBeforeSync = 2 * Constants.Size.Megabyte;
            options.MaxUnsyncedBytesBeforeMandatorySync = 8 * Constants.Size.Megabyte;
            options.EnableJournalPoolPrewarming = false;

            using var env = new StorageEnvironment(options);
            env.Options.ForTestingPurposesOnly().OnJournalWrite = (_, _) => Thread.Sleep(JournalWriteSleepMs); // slow-device class

            var streamFor = TimeSpan.FromMinutes(ctx.Minutes * 0.7);
            using var stop = new CancellationTokenSource(streamFor);
            var token = stop.Token;
            Exception failure = null;

            var writer = new Thread(() =>
            {
                try
                {
                    var value = Encoding.ASCII.GetBytes(new string('v', 1024));
                    long n = 0;
                    while (token.IsCancellationRequested == false)
                    {
                        using var tx = env.WriteTransaction();
                        var tree = tx.CreateTree("data");
                        for (var i = 0; i < 30; i++)
                            tree.Add($"k/{n++:D10}", value);
                        tx.Commit();
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            });

            var monitor = new Thread(() =>
            {
                try
                {
                    var sp = Stopwatch.StartNew();
                    var lastFlushed = env.Journal.Applicator.LastFlushedTransactionId;
                    var lastAdvance = sp.Elapsed;
                    var peakFiles = 0;
                    long peakUnsynced = 0;
                    var lastReport = -1;
                    while (token.IsCancellationRequested == false)
                    {
                        Thread.Sleep(2000);
                        var flushed = env.Journal.Applicator.LastFlushedTransactionId;
                        var files = env.Journal.Files.Count;
                        var unsynced = env.Journal.Applicator.TotalWrittenButUnsyncedBytes;
                        peakFiles = Math.Max(peakFiles, files);
                        peakUnsynced = Math.Max(peakUnsynced, unsynced);

                        if (flushed > lastFlushed)
                        {
                            lastFlushed = flushed;
                            lastAdvance = sp.Elapsed;
                        }
                        else if ((sp.Elapsed - lastAdvance).TotalSeconds > MaxStallSeconds)
                        {
                            throw new InvalidOperationException($"flusher starved: LastFlushedTransactionId stuck at {flushed} for {MaxStallSeconds}s while commits continue (journal files {files}, unsynced {unsynced / Constants.Size.Megabyte}MB)");
                        }

                        if (files > MaxJournalFiles)
                            throw new InvalidOperationException($"journal files accumulate without bound: {files} (flushed tx {flushed}, unsynced {unsynced / Constants.Size.Megabyte}MB)");
                        if (unsynced > MaxUnsyncedBytes)
                            throw new InvalidOperationException($"unsynced bytes accumulate without bound: {unsynced / Constants.Size.Megabyte}MB (journal files {files})");

                        var half = (int)(sp.Elapsed.TotalSeconds / 30);
                        if (half != lastReport)
                        {
                            lastReport = half;
                            Console.WriteLine($"  t+{sp.Elapsed.TotalSeconds:0}s: flushed tx {flushed}, journal files {files} (peak {peakFiles}), unsynced {unsynced / Constants.Size.Kilobyte}KB (peak {peakUnsynced / Constants.Size.Kilobyte}KB)");
                        }
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref failure, e, null);
                    stop.Cancel();
                }
            });

            writer.Start();
            monitor.Start();
            writer.Join();
            monitor.Join();

            if (failure != null)
            {
                ctx.Fail(failure.ToString());
                return ctx.Finish();
            }

            // the stream ended: flush + sync must complete on their own
            var drain = Stopwatch.StartNew();
            while (drain.Elapsed.TotalSeconds < DrainTimeoutSeconds)
            {
                if (env.Journal.Applicator.TotalWrittenButUnsyncedBytes == 0 && env.Journal.Files.Count <= 2)
                    break;
                Thread.Sleep(1000);
            }

            var filesLeft = env.Journal.Files.Count;
            var unsyncedLeft = env.Journal.Applicator.TotalWrittenButUnsyncedBytes;
            if (unsyncedLeft != 0 || filesLeft > 2)
                ctx.Fail($"after the write stream ended, flush/sync did not complete within {DrainTimeoutSeconds}s: {filesLeft} journal files, {unsyncedLeft / Constants.Size.Kilobyte}KB unsynced (the yielded-flush idle retry did not fire)");
            else
                Console.WriteLine($"  stream ended; flush+sync completed in {drain.Elapsed.TotalSeconds:0}s ({filesLeft} journal files left)");

            return ctx.Finish();
        }
    }
}
