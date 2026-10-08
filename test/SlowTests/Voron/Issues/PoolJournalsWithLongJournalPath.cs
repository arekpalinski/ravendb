using System;
using System.IO;
using System.Threading;
using FastTests;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

public class PoolJournalsWithLongJournalPath : NoDisposalNeeded
{
    public PoolJournalsWithLongJournalPath(ITestOutputHelper output) : base(output)
    {
    }

    // the zeroed pool journal is <dir>\recyclable-journal.<19 digits>.tmp, 43 chars more than the dir: 259 chars for 216, 260 for 217.
    // A dir under 260 chars gets no \\?\ prefix, while the journals themselves (28 chars more) still fit in MAX_PATH.
    [RavenMultiplatformTheory(RavenTestCategory.Voron, RavenPlatform.Windows)]
    [InlineData(216)]
    [InlineData(217)]
    public void HalfFullJournalPreparesAPoolJournal(int journalDirLength)
    {
        string root = RavenTestHelper.NewDataPath(nameof(PoolJournalsWithLongJournalPath), journalDirLength, forceCreateDir: true);
        string dataDir = Path.Combine(root, new string('p', journalDirLength - root.Length - 1 - @"\Journals".Length));
        try
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dataDir);
            Assert.Equal(journalDirLength, options.JournalPath.FullPath.Length);

            var prepared = new ManualResetEventSlim();
            options.ForTestingPurposesOnly().AfterJournalZeroing = prepared.Set;

            using (var env = new StorageEnvironment(options))
            {
                env.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;

                var value = new byte[16 * 1024];
                Random.Shared.NextBytes(value);

                // stop writing as soon as a preparation ran, so no journal rollover takes the pool file
                for (int i = 0; i < 64 && prepared.IsSet == false; i++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        tx.CreateTree("t").Add("k" + i, value);
                        tx.Commit();
                    }

                    prepared.Wait(TimeSpan.FromSeconds(1));
                }

                Assert.True(prepared.IsSet, "a half-full journal should start a pool journal preparation");
                Assert.NotEmpty(Directory.GetFiles(options.JournalPath.FullPath, "recyclable-journal.*"));
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(root);
        }
    }
}
