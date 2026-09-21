using System;
using System.Diagnostics;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // One environment, one writer doing ~1MB transactions as fast as it can, the real flusher and
    // syncer. Prints how far TotalWrittenButUnsyncedBytes gets above
    // so the two can be compared directly - it only touches StorageEnvironment and options.
    public class UnsyncedBacklogUnderSustainedWrites : StorageTest
    {
        public UnsyncedBacklogUnderSustainedWrites(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void MeasureUnsyncedBacklog()
        {
            var dir = Path.Combine(DataDir, "backlog");
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
            Directory.CreateDirectory(dir);

            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.MaxLogFileSize = 4 * Constants.Size.Megabyte;

            using var env = new StorageEnvironment(options);

            var rng = new Random(2);
            var sp = Stopwatch.StartNew();
            long peak = 0, txs = 0, bytes = 0;
            var nextReport = TimeSpan.FromSeconds(10);

            // bounded by bytes as well as time: this writer can push tens of GB in a couple of minutes
            while (sp.Elapsed < TimeSpan.FromSeconds(90) && bytes < 6L * Constants.Size.Gigabyte)
            {
                var size = 512 * Constants.Size.Kilobyte + rng.Next(Constants.Size.Megabyte);
                var value = new byte[size];

                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("items").Add($"k/{txs:D8}", value);
                    tx.Commit();
                }

                txs++;
                bytes += size;

                var unsynced = env.Journal.Applicator.TotalWrittenButUnsyncedBytes;
                if (unsynced > peak)
                    peak = unsynced;

                if (sp.Elapsed > nextReport)
                {
                    nextReport += TimeSpan.FromSeconds(10);
                    Console.WriteLine($"t+{sp.Elapsed.TotalSeconds:0}s txs={txs} wrote={bytes / Constants.Size.Megabyte}MB unsynced now={unsynced / Constants.Size.Megabyte}MB peak={peak / Constants.Size.Megabyte}MB journals={env.Journal.Files.Count}");
                }
            }

            Console.WriteLine($"RESULT txs={txs} wrote={bytes / Constants.Size.Megabyte}MB peakUnsynced={peak / Constants.Size.Megabyte}MB (mandatory ceiling 16MB)");
            Assert.True(peak < 256L * Constants.Size.Megabyte,
                $"unsynced backlog reached {peak / Constants.Size.Megabyte}MB with default sync settings after writing {bytes / Constants.Size.Megabyte}MB in {txs} transactions");
        }
    }
}
