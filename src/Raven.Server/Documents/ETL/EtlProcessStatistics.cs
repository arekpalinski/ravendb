using System;
using Raven.Client.Util;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL
{
    public sealed class EtlProcessStatistics : IDynamicJson
    {
        private readonly string _processTag;
        private readonly string _processName;
        private readonly DatabaseNotificationCenter _notificationCenter;
        private readonly EtlErrorsStorage _etlErrorsStorage;

        private readonly OnDisposeActions _onDisposeActions;

        private bool _preventFromAddingAlertsToNotificationCenter;

        public EtlProcessStatistics()
        {
            // for deserialization
        }
        
        public EtlProcessStatistics(string processTag, string processName, EtlErrorsStorage etlErrorsStorage, DatabaseNotificationCenter notificationCenter)
        {
            _processTag = processTag;
            _processName = processName;
            _notificationCenter = notificationCenter;
            _etlErrorsStorage = etlErrorsStorage;
            TransformationErrorsInCurrentBatch = new EtlErrorsDetails();
            LastLoadErrorsInCurrentBatch = new EtlErrorsDetails();
            LastSlowSqlWarningsInCurrentBatch = new SlowSqlDetails();
            _onDisposeActions = new OnDisposeActions(this);
            AverageErrorsRatio = new TimeAgnosticEwma();
            HealthStatus = EtlTaskHealthStatus.Healthy;
        }

        public string LastChangeVector { get; set; }

        public long LastProcessedEtag { get; set; }

        public DateTime? LastTransformationErrorTime { get; private set; }

        public DateTime? LastLoadErrorTime { get; private set; }

        public int TransformationErrors { get; set; }

        public int TransformationSuccesses { get; set; }

        public int LoadErrors { get; set; }

        public int LoadSuccesses { get; private set; }

        public int LoadSuccessesInCurrentBatch { get; private set; }

        public AlertRaised LastAlert { get; set; }

        public EtlErrorsDetails TransformationErrorsInCurrentBatch { get; }

        public EtlErrorsDetails LastLoadErrorsInCurrentBatch { get; }

        public SlowSqlDetails LastSlowSqlWarningsInCurrentBatch { get; }

        public bool WasLatestLoadSuccessful { get; set; }
        
        public TimeAgnosticEwma AverageErrorsRatio { get; }
        
        public long BatchErrors { get; set; }
        public long BatchSuccesses { get; set; }
        
        public EtlTaskHealthStatus HealthStatus { get; private set; }
        private bool ExplicitFail { get; set; }
        public DateTime? NextBatchRetryTime { get; set; }

        public void TransformationSuccess()
        {
            TransformationSuccesses++;
        }

        public IDisposable NewBatch()
        {
            TransformationErrorsInCurrentBatch.Errors.Clear();
            LastLoadErrorsInCurrentBatch.Errors.Clear();
            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
            LoadSuccessesInCurrentBatch = 0;
            BatchErrors = 0;
            BatchSuccesses = 0;
            
            return _onDisposeActions;
        }

        internal void UpdateHealthStatusOnBatchCompletion()
        {
            var previousStatus = HealthStatus;
            
            if (ExplicitFail)
            {
                HealthStatus = EtlTaskHealthStatus.Failed;
            }

            else
            {
                var errorsEwma = AverageErrorsRatio.GetRate();
            
                HealthStatus = errorsEwma switch
                {
                    > 0.9 => EtlTaskHealthStatus.Failed,
                    > 0.5 => EtlTaskHealthStatus.Disrupted,
                    > 0.1 => EtlTaskHealthStatus.Impaired,
                    _ => EtlTaskHealthStatus.Healthy
                };
            }

            if (HealthStatus == EtlTaskHealthStatus.Healthy)
            {
                _notificationCenter.EtlNotifications.ClearTaskHealthChangeNotification(_processTag, _processName);
                return;
            }
            
            if (HealthStatus != previousStatus)
                _notificationCenter.EtlNotifications.AddTaskHealthChangeNotification(_processTag, _processName, HealthStatus);
        }

        internal void SetExplicitFail(string message)
        {
            ExplicitFail = true;

            RecordConfigurationError(message);
        }

        public void RecordTransformationError(Exception e, LazyStringValue documentId)
        {
            var now = SystemTime.UtcNow;

            var partialError = new PartialEtlError()
            {
                CreatedAt = now,
                EtlTaskName = _processName,
                DocumentId = documentId,
                Step = EtlErrorStep.TransformationError,
                Severity = EtlErrorSeverity.Low
            };
            
            _etlErrorsStorage.StorePartialError(partialError);
            
            TransformationErrors++;
            BatchErrors++;

            LastTransformationErrorTime = now;

            TransformationErrorsInCurrentBatch.Add(new EtlErrorInfo
            {
                Date = now,
                DocumentId = documentId,
                Error = e.ToString()
            });
        }

        public void RecordPartialLoadError(string error, LazyStringValue documentId, int count = 1)
        {
            var now = SystemTime.UtcNow;
            
            var partialError = new PartialEtlError()
            {
                CreatedAt = now,
                EtlTaskName = _processName,
                DocumentId = documentId,
                Step = EtlErrorStep.TransformationError,
                Severity = EtlErrorSeverity.Low
            };
            
            _etlErrorsStorage.StorePartialError(partialError);
            WasLatestLoadSuccessful = false;

            LoadErrors += count; 
            BatchErrors += count;
            
            LastLoadErrorTime = now;

            LastLoadErrorsInCurrentBatch.Add(new EtlErrorInfo
            {
                Date = now,
                DocumentId = documentId,
                Error = error
            });
        }

        private void RecordConfigurationError(string message)
        {
            var now = SystemTime.UtcNow;

            var etlError = new EtlError()
            {
                CreatedAt = now,
                EtlTaskName = _processName,
                AffectedDocumentsCount = 0,
                Step = EtlErrorStep.ConfigurationError,
                Severity = EtlErrorSeverity.High,
                Message = message
            };
            
            _etlErrorsStorage.StoreError(etlError);
        }

        public void ThrowLoadError(string error, int count)
        {
            var now = SystemTime.UtcNow;

            var etlError = new EtlError()
            {
                CreatedAt = now,
                EtlTaskName = _processName,
                AffectedDocumentsCount = count,
                Step = EtlErrorStep.LoadError,
                Severity = EtlErrorSeverity.Low
            };
            
            _etlErrorsStorage.StoreError(etlError);
            
            LastLoadErrorTime = now;
            BatchErrors += count;

            LastLoadErrorsInCurrentBatch.Add(new EtlErrorInfo
            {
                Date = now,
                Error = error
            });
        }

        public void RecordSlowSql(SlowSqlStatementInfo slowSql)
        {
            LastSlowSqlWarningsInCurrentBatch.Add(slowSql);
        }

        public void LoadSuccess(int items)
        {
            WasLatestLoadSuccessful = true;
            LoadSuccesses += items;
            LoadSuccessesInCurrentBatch += items;
        }

        private void CreateAlertIfAnySlowSqls()
        {
            if (LastSlowSqlWarningsInCurrentBatch.Statements.Count == 0 || _preventFromAddingAlertsToNotificationCenter)
                return;

            _notificationCenter.EtlNotifications.AddSlowSqlWarnings(_processTag, _processName, LastSlowSqlWarningsInCurrentBatch.Statements);

            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(LastAlert)] = LastAlert?.ToJson(),
                [nameof(LastTransformationErrorTime)] = LastTransformationErrorTime,
                [nameof(LastLoadErrorTime)] = LastLoadErrorTime,
                [nameof(LastProcessedEtag)] = LastProcessedEtag,
                [nameof(TransformationSuccesses)] = TransformationSuccesses,
                [nameof(TransformationErrors)] = TransformationErrors,
                [nameof(LoadSuccesses)] = LoadSuccesses,
                [nameof(LoadErrors)] = LoadErrors,
                [nameof(AverageErrorsRatio)] = AverageErrorsRatio.GetRate(),
                [nameof(HealthStatus)] = HealthStatus,
                [nameof(NextBatchRetryTime)] =  NextBatchRetryTime
            };
            return json;
        }

        public override string ToString()
        {
            return $"{nameof(LastProcessedEtag)}: {LastProcessedEtag} " +
                   $"{nameof(LastTransformationErrorTime)}: {LastTransformationErrorTime} " +
                   $"{nameof(LastLoadErrorTime)}: {LastLoadErrorTime} " +
                   $"{nameof(TransformationSuccesses)}: {TransformationSuccesses} " +
                   $"{nameof(TransformationErrors)}: {TransformationErrors} " +
                   $"{nameof(LoadSuccesses)}: {LoadSuccesses} " +
                   $"{nameof(LoadErrors)}: {LoadErrors}";
        }

        public void Reset()
        {
            LastProcessedEtag = 0;
            LastTransformationErrorTime = null;
            LastLoadErrorTime = null;
            TransformationSuccesses = 0;
            TransformationErrors = 0;
            LoadSuccesses = 0;
            LoadSuccessesInCurrentBatch = 0;
            LoadErrors = 0;
            BatchErrors = 0;
            BatchSuccesses = 0;
            LastChangeVector = null;
            LastAlert = null;
            TransformationErrorsInCurrentBatch.Errors.Clear();
            LastLoadErrorsInCurrentBatch.Errors.Clear();
            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
        }

        private sealed class OnDisposeActions : IDisposable
        {
            private readonly EtlProcessStatistics _parent;

            public OnDisposeActions(EtlProcessStatistics parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent.CreateAlertIfAnySlowSqls();
                _parent.AverageErrorsRatio.UpdateOnBatchCompletion(_parent.BatchErrors, _parent.BatchErrors + _parent.BatchSuccesses);
                _parent.UpdateHealthStatusOnBatchCompletion();
            }
        }

        public IDisposable PreventFromAddingAlertsToNotificationCenter()
        {
            _preventFromAddingAlertsToNotificationCenter = true;

            return new DisposableAction(() => _preventFromAddingAlertsToNotificationCenter = false);
        }
    }
}
