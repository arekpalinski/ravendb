using System.IO;
using FastTests.Voron;
using Voron;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // DeviceWriteBudget.GetForDevice caches one instance per device id, and the instance captures the
    // three thresholds handed to it by whoever created it. Every later environment on the same device
    // gets that instance back, thresholds and all - so its own SyncWritebackBarrierCostThresholdTicks,
    // SyncWritebackDrainQueueDepthThreshold and PipelineJournalWritesAboveLatencyInTicks are dropped on
    // the floor without a word.
    public class DeviceWriteBudgetFirstEnvWins : StorageTest
    {
        public DeviceWriteBudgetFirstEnvWins(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EnvironmentsOnTheSameDeviceMustNotSilentlyInheritTheFirstOnesThresholds()
        {
            var first = Path.Combine(DataDir, "first");
            var second = Path.Combine(DataDir, "second");
            Directory.CreateDirectory(first);
            Directory.CreateDirectory(second);

            var firstOptions = StorageEnvironmentOptions.ForPathForTests(first);
            firstOptions.PipelineJournalWritesAboveLatencyInTicks = 5_000_000;
            firstOptions.SyncWritebackDrainQueueDepthThreshold = 99;

            var secondOptions = StorageEnvironmentOptions.ForPathForTests(second);
            // what a test does when it wants to force the pipelined path
            secondOptions.PipelineJournalWritesAboveLatencyInTicks = 0;
            secondOptions.SyncWritebackDrainQueueDepthThreshold = 1;

            using var a = new StorageEnvironment(firstOptions);
            using var b = new StorageEnvironment(secondOptions);

            Assert.NotNull(a.Options.DeviceWriteBudget);
            Assert.NotNull(b.Options.DeviceWriteBudget);

            Assert.False(ReferenceEquals(a.Options.DeviceWriteBudget, b.Options.DeviceWriteBudget),
                "the second environment was handed the first one's DeviceWriteBudget, so its own PipelineJournalWritesAboveLatencyInTicks / SyncWriteback* settings were silently discarded");
        }
    }
}
