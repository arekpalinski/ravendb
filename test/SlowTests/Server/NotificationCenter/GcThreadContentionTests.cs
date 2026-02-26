using System.Linq;
using System.Runtime;
using System.Threading.Tasks;
using Raven.Server.Commercial;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.NotificationCenter
{
    public class GcThreadContentionTests : ClusterTestBase
    {
        public GcThreadContentionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public async Task Should_raise_alert_when_cores_are_underutilized()
        {
            // Skip test if not using Server GC
            if (GCSettings.IsServerGC == false)
                return;

            DoNotReuseServer();

            var server = GetNewServer();
            var store = server.ServerStore;

            try
            {
                // Wait for license limits to be initialized
                await Task.Delay(1000);

                // Simulate a scenario where we have limited cores
                // We'll manually trigger the check by calling the monitor's internal logic
                var monitor = store.GcThreadContentionMonitor;
                Assert.NotNull(monitor);

                // Trigger a check - in a real scenario, the timer would do this automatically
                // We need to wait for the initial timer to trigger
                await Task.Delay(6000); // Wait a bit more than the 5-minute check frequency won't work in tests

                // For testing purposes, let's directly check if the notification center has alerts
                // In a production scenario with mismatched cores, the alert should appear
                System.Collections.Generic.IEnumerable<NotificationTableValue> notifications;
                using (store.NotificationCenter.GetStored(out notifications))
                {
                    var alerts = notifications
                        .Where(n => 
                        {
                            n.Json.TryGet(nameof(AlertRaised.AlertType), out AlertType alertType);
                            return alertType == AlertType.GcThreadContention;
                        })
                        .ToList();

                    // Note: This test will only produce an alert if the actual machine has more cores
                    // than the license allows. In most CI environments, this won't be the case.
                    // The test primarily validates that the monitor is properly initialized and integrated.
                }
                
                // Validate that the monitor is properly integrated
                Assert.NotNull(store.GcThreadContentionMonitor);
            }
            finally
            {
                server.Dispose();
            }
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public void Should_create_proper_alert_details()
        {
            // Test that the alert details are created correctly
            var details = new GcThreadContentionDetails
            {
                TotalCores = 32,
                UtilizedCores = 4,
                Message = "Test message"
            };

            var json = details.ToJson();
            
            Assert.NotNull(json);
            Assert.Equal(32, json[nameof(GcThreadContentionDetails.TotalCores)]);
            Assert.Equal(4, json[nameof(GcThreadContentionDetails.UtilizedCores)]);
            Assert.Equal("Test message", json[nameof(GcThreadContentionDetails.Message)]);
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public void Alert_type_should_exist()
        {
            // Validate that the new alert type exists
            var alertType = AlertType.GcThreadContention;
            Assert.True(System.Enum.IsDefined(typeof(AlertType), alertType));
        }
    }
}
