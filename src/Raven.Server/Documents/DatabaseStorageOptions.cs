using System;
using System.Threading;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Utils;
using Sparrow.Server;
using Voron;
using Voron.Exceptions;

namespace Raven.Server.Documents
{
    public class DatabaseStorageOptions
    {
        private readonly NodeTagHolder _nodeTagHolder;

        private DatabaseStorageOptions(string databaseName, RavenConfiguration configuration, bool is32Bits, NodeTagHolder nodeTagHolder, byte[] masterKey,
            DocumentsChanges changes, IoChangesNotifications ioChanges, MetricCounters metrics, SystemTime time, 
            CatastrophicFailureNotification catastrophicFailureNotification, EventHandler<RecoveryErrorEventArgs> handleOnDatabaseRecoveryError,
            EventHandler<NonDurabilitySupportEventArgs> handleNonDurableFileSystemError, EventHandler<DataIntegrityErrorEventArgs> handleOnDatabaseIntegrityErrorOfAlreadySyncedData,
            CancellationToken databaseShutdown)
        {
            _nodeTagHolder = nodeTagHolder;
            Name = databaseName;
            Configuration = configuration;
            Is32Bits = is32Bits;
            MasterKey = masterKey;
            Changes = changes;
            IoChanges = ioChanges;
            Metrics = metrics;
            Time = time;
            CatastrophicFailureNotification = catastrophicFailureNotification;
            HandleOnDatabaseRecoveryError = handleOnDatabaseRecoveryError;
            HandleNonDurableFileSystemError = handleNonDurableFileSystemError;
            HandleOnDatabaseIntegrityErrorOfAlreadySyncedData = handleOnDatabaseIntegrityErrorOfAlreadySyncedData;
            DatabaseShutdown = databaseShutdown;
        }

        public string Name { get; }

        public RavenConfiguration Configuration { get; }
        
        public bool Is32Bits { get; set; }

        public string NodeTag => _nodeTagHolder.NodeTag;

        public byte[] MasterKey { get; }

        public IoChangesNotifications IoChanges { get; }

        public DocumentsChanges Changes { get; }

        public CatastrophicFailureNotification CatastrophicFailureNotification { get; }

        public EventHandler<RecoveryErrorEventArgs> HandleOnDatabaseRecoveryError { get; }

        public EventHandler<NonDurabilitySupportEventArgs> HandleNonDurableFileSystemError { get; }

        public EventHandler<DataIntegrityErrorEventArgs> HandleOnDatabaseIntegrityErrorOfAlreadySyncedData { get; }

        public SystemTime Time { get; }

        public MetricCounters Metrics { get; }

        public CancellationToken DatabaseShutdown { get; }

        public static DatabaseStorageOptions Create(string databaseName, RavenConfiguration configuration, bool is32Bits, NodeTagHolder nodeTag, byte[] masterKey,
            DocumentsChanges changes, IoChangesNotifications ioChanges, MetricCounters metrics,  SystemTime time,
            CatastrophicFailureNotification catastrophicFailureNotification, EventHandler<RecoveryErrorEventArgs> handleOnDatabaseRecoveryError,
            EventHandler<NonDurabilitySupportEventArgs> handleNonDurableFileSystemError,
            EventHandler<DataIntegrityErrorEventArgs> handleOnDatabaseIntegrityErrorOfAlreadySyncedData,
            CancellationToken databaseShutdown)
        {
            return new DatabaseStorageOptions(databaseName, configuration, is32Bits, nodeTag, masterKey, changes, ioChanges, metrics, time, catastrophicFailureNotification, handleOnDatabaseRecoveryError, handleNonDurableFileSystemError, handleOnDatabaseIntegrityErrorOfAlreadySyncedData, databaseShutdown);
        }

        public static DatabaseStorageOptions CreateForRecovery(string databaseName, RavenConfiguration configuration, bool is32Bits, NodeTagHolder nodeTag, byte[] masterKey)
        {
            return new DatabaseStorageOptions(databaseName, configuration, is32Bits, nodeTag, masterKey, new DocumentsChanges(), new IoChangesNotifications(), new MetricCounters(), new SystemTime(), new CatastrophicFailureNotification((guid, s, ex, r) => {}), 
                (s, e) => { }, (s, e) => { }, (s, e) => { }, CancellationToken.None);
        }
    }
}
