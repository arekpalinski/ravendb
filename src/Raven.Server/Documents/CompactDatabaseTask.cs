using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Compaction;

namespace Raven.Server.Documents
{
    public sealed class CompactDatabaseTask
    {
        private readonly RavenLogger _logger;

        private readonly ServerStore _serverStore;
        private readonly string _database;
        private readonly PathSetting _compactionTempRoot;
        private readonly bool _deleteOriginalBeforeCopyBack;
        private CancellationToken _token;
        private bool _isCompactionInProgress;
        private bool _originalDeleted;
        private DocumentDatabase.TestingStuff _forTestingPurposes;

        public CompactDatabaseTask(ServerStore serverStore, string database, CancellationToken token, PathSetting compactionTempRoot = null, bool deleteOriginalBeforeCopyBack = false)
        {
            _serverStore = serverStore;
            _database = database;
            _compactionTempRoot = compactionTempRoot;
            _deleteOriginalBeforeCopyBack = deleteOriginalBeforeCopyBack;
            _token = token;
            _logger = RavenLogManager.Instance.GetLoggerForDatabase<CompactDatabaseTask>(database);
        }

        public async Task Execute(Action<IOperationProgress> onProgress, CompactionResult result)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Database '{_database}' cannot be compacted because compaction is already in progress.");

            result.AddMessage($"Started database compaction for {_database}");
            onProgress?.Invoke(result.Progress);

            _isCompactionInProgress = true;
            bool done = false;
            string basePath = null;
            string compactDirectory = null;
            string tmpDirectory = null;
            string compactTempDirectory = null;
            byte[] encryptionKey = null;
            try
            {
                var documentDatabase = await _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(_database);
                var configuration = _serverStore.DatabasesLandlord.CreateDatabaseConfiguration(_database);

                _forTestingPurposes = documentDatabase.ForTestingPurposes;

                DatabaseRecord databaseRecord = documentDatabase.ReadDatabaseRecord();


                // save the key before unloading the database (it is zeroed when disposing DocumentDatabase). 
                if (documentDatabase.MasterKey != null)
                    encryptionKey = documentDatabase.MasterKey.ToArray();

                var loggingResource = LoggingResource.Database(_database);

                using (await _serverStore.DatabasesLandlord.UnloadAndLockDatabase(_database, "it is being compacted"))
                using (var src = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications
                    {
                        DisableIoMetrics = true
                    },
                new CatastrophicFailureNotification((endId, path, exception, stacktrace) => throw new InvalidOperationException($"Failed to compact database {_database} ({path}), StackTrace='{stacktrace}'", exception)), loggingResource))
                {
                    documentDatabase.ForTestingPurposes?.CompactionAfterDatabaseUnload?.Invoke();

                    InitializeOptions(src, configuration, documentDatabase, encryptionKey);
                    DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(src, configuration.Storage, documentDatabase.Name, DirectoryExecUtils.EnvironmentType.Compaction, _logger);

                    basePath = configuration.Core.DataDirectory.FullPath;

                    // when the database directory is a symlink / junction, operate on the real directory: the '-compacting' / '-old' siblings
                    // stay on the device that actually holds the data and the link itself is never renamed or deleted during the swap
                    basePath = IOExtensions.TryGetResolvedDirectoryLinkTarget(basePath) ?? basePath;

                    compactDirectory = _compactionTempRoot != null
                        ? _compactionTempRoot.Combine(_database + "-compacting").FullPath
                        : basePath + "-compacting";
                    tmpDirectory = basePath + "-old";

                    EnsureDirectoriesPermission(basePath, compactDirectory, tmpDirectory);

                    IOExtensions.DeleteDirectory(compactDirectory);
                    IOExtensions.DeleteDirectory(tmpDirectory);

                    configuration.Core.DataDirectory = new PathSetting(compactDirectory);

                    if (configuration.Storage.TempPath != null)
                    {
                        compactTempDirectory = configuration.Storage.TempPath.FullPath + "-temp-compacting";

                        EnsureDirectoriesPermission(compactTempDirectory);
                        IOExtensions.DeleteDirectory(compactTempDirectory);

                        configuration.Storage.TempPath = new PathSetting(compactTempDirectory);
                    }

                    var revisionsPrefix = CollectionName.GetTablePrefix(CollectionTableType.Revisions);
                    var collectionsPrefix = CollectionName.GetTablePrefix(CollectionTableType.Documents);

                    var compressedCollectionsTableNames = databaseRecord.DocumentsCompression?.Collections
                        .Select(name => new CollectionName(name).GetTableName(CollectionTableType.Documents))
                        .ToHashSet(StringComparer.OrdinalIgnoreCase);

                    using (var dst = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications
                        {
                            DisableIoMetrics = true
                        },
                        new CatastrophicFailureNotification((envId, path, exception, stacktrace) => throw new InvalidOperationException($"Failed to compact database {_database} ({path}). StackTrace='{stacktrace}'", exception)), loggingResource))
                    {
                        InitializeOptions(dst, configuration, documentDatabase, encryptionKey);
                        DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(dst, configuration.Storage, documentDatabase.Name, DirectoryExecUtils.EnvironmentType.Compaction, _logger);

                        _token.ThrowIfCancellationRequested();
                        StorageCompaction.Execute(src, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dst, progressReport =>
                        {
                            result.Progress.TreeProgress = progressReport.TreeProgress;
                            result.Progress.TreeTotal = progressReport.TreeTotal;
                            result.Progress.TreeName = progressReport.TreeName;
                            result.Progress.GlobalProgress = progressReport.GlobalProgress;
                            result.Progress.GlobalTotal = progressReport.GlobalTotal;
                            result.AddMessage(progressReport.Message);
                            onProgress?.Invoke(result.Progress);
                        }, (name, schema) =>
                        {
                            bool isRevision = name.StartsWith(revisionsPrefix,StringComparison.OrdinalIgnoreCase);
                            bool isCollection = name.StartsWith(collectionsPrefix, StringComparison.OrdinalIgnoreCase);
                            schema.Compressed = 
                                (isRevision && databaseRecord.DocumentsCompression?.CompressRevisions == true) ||
                                (isCollection && databaseRecord.DocumentsCompression?.CompressAllCollections == true) ||
                                compressedCollectionsTableNames?.Contains(name) == true;
                        },_token);
                    }

                    result.TreeName = null;

                    _token.ThrowIfCancellationRequested();

                    EnsureDirectoriesPermission(basePath, compactDirectory, tmpDirectory);
                    IOExtensions.DeleteDirectory(tmpDirectory);

                    if (_compactionTempRoot != null)
                    {
                        result.AddMessage($"Copying compacted data from '{compactDirectory}' back to '{basePath}'");
                        onProgress?.Invoke(result.Progress);
                    }

                    SwitchDatabaseDirectories(basePath, tmpDirectory, compactDirectory);
                    done = true;
                }
            }
            catch (Exception e)
            {
                var message = $"Failed to execute compaction for {_database}";

                if (_originalDeleted && Directory.Exists(compactDirectory))
                    message += $". The original data was already deleted and the compacted data is preserved at '{compactDirectory}'. To restore the database, move its contents to '{basePath}' manually";

                throw new InvalidOperationException(message, e);
            }
            finally
            {
                // unless the swap failed after the original data was purged - then the compacted data is the only remaining copy, keep it for manual recovery
                if (_originalDeleted == false || done)
                    IOExtensions.DeleteDirectory(compactDirectory);

                if (done)
                {
                    IOExtensions.DeleteDirectory(tmpDirectory);

                    if (compactTempDirectory != null)
                        IOExtensions.DeleteDirectory(compactTempDirectory);
                }
                _isCompactionInProgress = false;
                if (encryptionKey != null)
                    Sodium.ZeroBuffer(encryptionKey);
            }
        }

        private static void EnsureDirectoriesPermission(params string[] directories)
        {
            var missingPermissions = new List<string>();

            foreach (var directory in directories)
            {
                if (IOExtensions.EnsureReadWritePermissionForDirectory(directory) == false)
                {
                    missingPermissions.Add(directory);
                }
            }

            if (missingPermissions.Count > 0)
            {
                throw new UnauthorizedAccessException(
                    $"Couldn't gain read/write access to the following directories:{Environment.NewLine}{string.Join(Environment.NewLine, missingPermissions)}");
            }
        }

        private static void InitializeOptions(StorageEnvironmentOptions options, RavenConfiguration configuration, DocumentDatabase documentDatabase, byte[] key)
        {
            options.ForceUsing32BitsPager = configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = documentDatabase.Configuration.Storage.EnablePrefetching;
            options.DiscardVirtualMemory = documentDatabase.Configuration.Storage.DiscardVirtualMemory;
            options.UseSequentialReadAheadHintForJournalRecovery = documentDatabase.Configuration.Storage.UseSequentialReadAheadHintForJournalRecovery;
            options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
            options.OnRecoverableFailure += documentDatabase.HandleRecoverableFailure;
            options.OnRecoveryError += documentDatabase.HandleOnDatabaseRecoveryError;
            options.OnIntegrityErrorOfAlreadySyncedData += documentDatabase.HandleOnDatabaseIntegrityErrorOfAlreadySyncedData;
            options.CompressTxAboveSizeInBytes = configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.TimeToSyncAfterFlushInSec = (int)configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
            options.Encryption.MasterKey = key?.ToArray(); // clone 
            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = documentDatabase.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = documentDatabase.Configuration.Storage.SyncJournalsCountThreshold;
            options.SkipChecksumValidationOnDatabaseLoading = documentDatabase.Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.MaxNumberOfRecyclableJournals = documentDatabase.Configuration.Storage.MaxNumberOfRecyclableJournals;
        }

        private void SwitchDatabaseDirectories(string basePath, string backupDirectory, string compactDirectory)
        {
            var forceCrossDeviceMove = _forTestingPurposes?.CompactionForceCrossDeviceMove == true;

            var moves = new (string Src, string Dst, bool Optional)[]
            {
                (basePath, backupDirectory, false),
                (compactDirectory, basePath, false),
                (new PathSetting(backupDirectory).Combine("Indexes").FullPath, new PathSetting(basePath).Combine("Indexes").FullPath, true),
                (new PathSetting(backupDirectory).Combine("Configuration").FullPath, new PathSetting(basePath).Combine("Configuration").FullPath, true)
            };

            for (var i = 0; i < moves.Length; i++)
            {
                var moveDir = moves[i];

                if (moveDir.Optional && Directory.Exists(moveDir.Src) == false)
                    continue; // e.g. indexes stored outside the database directory (Indexing.StoragePath)

                try
                {
                    if (forceCrossDeviceMove)
                        IOExtensions.CopyDirectoryAndDeleteSource(moveDir.Src, moveDir.Dst);
                    else
                        IOExtensions.MoveDirectory(moveDir.Src, moveDir.Dst);
                }
                catch (Exception e)
                {
                    ThrowCantMoveDirectory(moveDir.Src, moveDir.Dst, e, basePath);
                }

                if (i == 0 && _deleteOriginalBeforeCopyBack)
                    PurgeOriginalDataFromBackupDirectory(backupDirectory);
            }
        }

        private void PurgeOriginalDataFromBackupDirectory(string backupDirectory)
        {
            // Indexes and Configuration are kept - they still have to be moved into the new database directory;
            // everything else is deleted so the database drive only needs to hold the compacted copy
            _originalDeleted = true;

            foreach (var entry in Directory.GetFileSystemEntries(backupDirectory))
            {
                var name = Path.GetFileName(entry);

                if (string.Equals(name, "Indexes", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(name, "Configuration", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (Directory.Exists(entry))
                    IOExtensions.DeleteDirectory(entry);
                else
                    File.Delete(entry);
            }

            _forTestingPurposes?.CompactionAfterOriginalDataPurge?.Invoke();
        }

        [DoesNotReturn]
        private static void ThrowCantMoveDirectory(string src, string dst, Exception e, string databasePath)
        {
            throw new IOException(
                $"Cannot move directory '{src}' to '{dst}'. Please verify the directory '{databasePath}' exists and contains the database's data before loading the database",
                e);
        }
    }
}
