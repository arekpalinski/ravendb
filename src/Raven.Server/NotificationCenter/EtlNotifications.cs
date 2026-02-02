using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public sealed class EtlNotifications
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        public EtlNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
        }

        public void AddSlowSqlWarnings(string processTag, string processName, Queue<SlowSqlStatementInfo> slowSqls)
        {
            var alert = GetOrCreatePerformanceHint<SlowSqlDetails>(processTag,
                processName,
                PerformanceHintType.SqlEtl_SlowSql,
                $"Slow SQL detected (last {SlowSqlDetails.MaxNumberOfStatements} statements are shown)",
                out var details);

            foreach (var slowSql in slowSqls)
            {
                details.Add(slowSql);
            }

            _notificationCenter.Add(alert);
        }

        public void AddTaskHealthChangeNotification(string processTag, string processName, EtlProcessHealthStatus status)
        {
            var alert = GetOrCreateAlert<EtlErrorsDetails>(processTag,
                processName,
                AlertType.Etl_HealthStatusChange,
                $"ETL task health status was changed to {status}.",
                out _);
            
            _notificationCenter.Add(alert);
        }

        private AlertRaised AddErrorAlert(Queue<EtlErrorInfo> errors, EtlErrorsDetails details, AlertRaised alert)
        {
            details.Update(errors);

            _notificationCenter.Add(alert);

            return alert;
        }

        private AlertRaised GetOrCreateAlert<T>(string processTag, string processName, AlertType etlAlertType, string message, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(etlAlertType == AlertType.Etl_LoadError || etlAlertType == AlertType.Etl_TransformationError || etlAlertType == AlertType.Etl_HealthStatusChange);

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(etlAlertType, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    details = GetDetails<T>(ntv);

                    return AlertRaised.Create(
                        _notificationCenter.Database,
                        $"{processTag}: '{processName}'",
                        message,
                        etlAlertType,
                        NotificationSeverity.Warning,
                        key: key,
                        details: details);
                }
            }
        }

        public AlertRaised GetAlert<T>(string processTag, string processName, AlertType etlAlertType)
            where T : INotificationDetails, new()
        {
            Debug.Assert(etlAlertType == AlertType.Etl_LoadError || etlAlertType == AlertType.Etl_TransformationError || etlAlertType == AlertType.Etl_HealthStatusChange);

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(etlAlertType, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                if (ntv == null)
                    return null;

                var details = GetDetails<T>(ntv);

                return AlertRaised.FromJson(key, ntv.Json, details);
            }
        }

        private PerformanceHint GetOrCreatePerformanceHint<T>(string processTag, string processName, PerformanceHintType etlHintType, string message, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(etlHintType == PerformanceHintType.SqlEtl_SlowSql);

            var key = $"{processTag}/{processName}";

            var id = PerformanceHint.GetKey(etlHintType, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    details = GetDetails<T>(ntv);

                    return PerformanceHint.Create(
                        _notificationCenter.Database,
                        $"{processTag}: '{processName}'",
                        message,
                        etlHintType,
                        NotificationSeverity.Warning,
                        source: key,
                        details: details);
                }
            }
        }

        private static T GetDetails<T>(NotificationTableValue ntv) where T : INotificationDetails, new()
        {
            if (ntv == null || ntv.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                return new T();

            return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<T>(detailsJson);
        }
    }
}
