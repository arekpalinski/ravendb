using System;
using System.Runtime;
using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Monitoring
{
    public sealed class GcThreadContentionMonitor : IDisposable
    {
        private const string Source = "gc-thread-contention";
        private static readonly TimeSpan InitialCheckDelay = TimeSpan.FromSeconds(30);

        // Alert when utilized cores are less than 50% of total cores
        private const double CoreUtilizationThreshold = 0.5;

        private readonly ServerStore _serverStore;
        private readonly ServerNotificationCenter _notificationCenter;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<GcThreadContentionMonitor>(nameof(GcThreadContentionMonitor));
        private Timer _timer;

        public GcThreadContentionMonitor(ServerStore serverStore, ServerNotificationCenter notificationCenter)
        {
            _serverStore = serverStore;
            _notificationCenter = notificationCenter;

            // Check once after startup - core configuration doesn't change at runtime
            _timer = new Timer(CheckGcThreadContention, null, InitialCheckDelay, Timeout.InfiniteTimeSpan);
        }

        private void CheckGcThreadContention(object state)
        {
            if (_notificationCenter.IsInitialized == false)
                return;

            try
            {
                var totalCores = ProcessorInfo.ProcessorCount;
                var utilizedCores = _serverStore.LicenseManager.GetNumberOfUtilizedCores();

                // Only alert if we're using Server GC and significantly underutilizing cores
                if (GCSettings.IsServerGC == false)
                    return;

                // Check if we're significantly underutilizing cores
                if (utilizedCores >= totalCores * CoreUtilizationThreshold)
                {
                    // Cores are adequately utilized, no alert needed
                    return;
                }

                // We have a potential GC thread contention issue
                RaiseAlert(totalCores, utilizedCores);
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Failed to check GC thread contention", e);
            }
        }

        private void RaiseAlert(int totalCores, int utilizedCores)
        {
            var details = new GcThreadContentionDetails
            {
                TotalCores = totalCores,
                UtilizedCores = utilizedCores,
                Message = $"Your server has {totalCores} CPU cores but is limited to use only {utilizedCores} core(s) by the license. " +
                          $"With Server GC enabled, .NET creates one GC thread per core ({totalCores} threads), but all threads are forced to run on {utilizedCores} core(s). " +
                          $"This can cause severe GC pauses due to thread contention. " +
                          $"To resolve this, configure the 'System.GC.HeapCount' setting in your Raven.Server.runtimeconfig.json file to match your utilized cores ({utilizedCores}). " +
                          $"For more information, see the documentation on runtime configuration."
            };

            var alert = AlertRaised.Create(
                null,
                "GC thread contention detected",
                details.Message,
                AlertType.GcThreadContention,
                NotificationSeverity.Warning,
                key: Source,
                details: details);

            _notificationCenter.Add(alert, updateExisting: true);

            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations($"GC thread contention detected: {totalCores} total cores but only {utilizedCores} utilized core(s). " +
                                  $"Consider configuring System.GC.HeapCount={utilizedCores} in runtimeconfig.json");
            }
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
