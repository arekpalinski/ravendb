using System;
using System.IO;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Platform;

namespace Voron.Recovery
{
    public class RecoveryStorage : IDisposable
    {
        private readonly string _recoveryDatabasePath;
        private DocumentsStorage _storage;
        private DocumentsOperationContext _context;
        private IDisposable _contextDisposal;
        private RecoveryTransactionManager _txManager;
        
        public RecoveryStorage(string recoveryDatabasePath)
        {
            _recoveryDatabasePath = recoveryDatabasePath;
        }

        public RecoveryStorage Initialize()
        {
            // TODO arek -encrypted db 

            var serverConfiguration = RavenConfiguration.CreateForServer("recovery-server");
            serverConfiguration.Initialize();

            string databaseName = $"recovered-db-{Guid.NewGuid()}";

            var databaseConfiguration = RavenConfiguration.CreateForDatabase(serverConfiguration, databaseName);
            databaseConfiguration.Initialize();

            databaseConfiguration.Core.DataDirectory = new PathSetting(_recoveryDatabasePath);

            _storage =  new DocumentsStorage(DatabaseStorageOptions.CreateForRecovery(databaseName, serverConfiguration, PlatformDetails.Is32Bits, new RecoveryNodeTagHolder("A"), null), s => { });
            _storage.Initialize();

            _contextDisposal = _storage.ContextPool.AllocateOperationContext(out _context);

            _txManager = new RecoveryTransactionManager(_context);

            return this;
        }

        public class RecoveryNodeTagHolder : NodeTagHolder
        {
            public RecoveryNodeTagHolder(string nodeTag)
            {
                NodeTag = nodeTag;
            }

            public override string NodeTag { get; }
        }

        public void PutDocument(Document document)
        {
            using (_txManager.EnsureWriteTransaction())
            {
                _storage.Put(_context, document.Id, null, document.Data, document.LastModified.Ticks);
            }
        }

        public void PutRevision(Document revision)
        {
            using (_txManager.EnsureWriteTransaction())
            {
                var document = _storage.Get(_context, revision.Id, throwOnConflict: false);
                if (document == null)
                {

                }

                _storage.RevisionsStorage.Put(_context, revision.Id, revision.Data, revision.Flags, NonPersistentDocumentFlags.None, null, revision.LastModified.Ticks);
            }
        }

        public void PutConflict(DocumentConflict conflict)
        {
            using (_txManager.EnsureWriteTransaction())
            {
                var document = _storage.Get(_context, conflict.Id, throwOnConflict: false);
                if (document == null)
                {

                }

                _storage.ConflictsStorage.AddConflict(_context, conflict.Id, conflict.LastModified.Ticks, conflict.Doc, conflict.ChangeVector, conflict.Collection, conflict.Flags, NonPersistentDocumentFlags.FromSmuggler); // TODO arek - FromSmuggler
            }
        }

        public void PutCounter(CounterGroupDetail counterGroup)
        {
            using (_txManager.EnsureWriteTransaction())
            {

            }
        }

        public void PutAttachment(FileStream stream, string hash, in long totalSize)
        {
            using (_txManager.EnsureWriteTransaction())
            {

            }
        }

        public void Dispose()
        {
            _contextDisposal?.Dispose();
            _storage?.Dispose();
        }
    }
}
