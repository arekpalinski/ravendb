using System;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications;

public class GcInfoNotificationSender : AbstractClusterDashboardNotificationSender
{
    public GcInfoNotificationSender(int widgetId, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
    {
    }

    protected override TimeSpan NotificationInterval { get; } = TimeSpan.FromSeconds(5);

    private GcInfoPayload.GcMemoryInfo ExtractGcMemoryInfoPayload(GCMemoryInfo info)
    {
        return new GcInfoPayload.GcMemoryInfo
        {
            Index = info.Index,
            Compacted = info.Compacted,
            Concurrent = info.Concurrent,
            PauseTimePercentage = info.PauseTimePercentage,
            PauseDurationsInMs =
            [
                info.PauseDurations[0].TotalMilliseconds,
                info.PauseDurations[1].TotalMilliseconds
            ],
            TotalHeapSizeAfterBytes = info.HeapSizeBytes,
            Gen0HeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[0].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[0].SizeAfterBytes,
                },
            Gen1HeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[1].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[1].SizeAfterBytes,
                },
            Gen2HeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[2].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[2].SizeAfterBytes,
                },
            LargeObjectHeapSize =
                new GcInfoPayload.GenerationInfoSize
                {
                    SizeBeforeBytes = info.GenerationInfo[3].SizeBeforeBytes, 
                    SizeAfterBytes = info.GenerationInfo[3].SizeAfterBytes,
                },
            PinnedObjectHeapSize = new GcInfoPayload.GenerationInfoSize
            {
                SizeBeforeBytes = info.GenerationInfo[4].SizeBeforeBytes, 
                SizeAfterBytes = info.GenerationInfo[4].SizeAfterBytes,
            },
        };
    }

    protected override AbstractClusterDashboardNotification CreateNotification()
    {
        var ephemeralGc = GC.GetGCMemoryInfo(GCKind.Ephemeral);
        var backgroundGc = GC.GetGCMemoryInfo(GCKind.Background);
        var fullBlockingGc = GC.GetGCMemoryInfo(GCKind.FullBlocking);

        return new GcInfoPayload
        {
            Ephemeral = ExtractGcMemoryInfoPayload(ephemeralGc),
            Background = ExtractGcMemoryInfoPayload(backgroundGc),
            FullBlocking = ExtractGcMemoryInfoPayload(fullBlockingGc),
        };
    }
}
