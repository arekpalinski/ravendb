using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;

namespace Voron.Recovery
{
    public class RecoveryStorage : IDisposable
    {
        private readonly string _recoveryDatabasePath;
        private readonly OrphansHandler _orphans;
        private DocumentsStorage _storage;
        private DocumentsOperationContext _context;
        private IDisposable _contextDisposal;
        private RecoveryTransactionManager _txManager;
        
        public RecoveryStorage(string recoveryDatabasePath)
        {
            _recoveryDatabasePath = recoveryDatabasePath;
            _orphans = new OrphansHandler(this);
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

            _storage =  new DocumentsStorage(DatabaseStorageOptions.CreateForRecovery(databaseName, databaseConfiguration, PlatformDetails.Is32Bits, new RecoveryNodeTagHolder("A"), null), s => { });
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

        public Document Get(string id)
        {
            using (_txManager.EnsureTransaction())
            {
                return _storage.Get(_context, id);
            }
        }

        public void PutDocument(Document document, string id = null)
        {
            using (_txManager.EnsureTransaction())
            {
                var metadata = document.Data.GetMetadata();
                if (metadata == null)
                    throw new Exception($"No metadata for {document.Id}, cannot recover this document");

                if (document.Id == "doc/4")
                {

                }

                var metadataDictionary = new MetadataAsDictionary(metadata);

                var hasRevisions = document.Flags.HasFlag(DocumentFlags.HasRevisions);
                var hasCounters = document.Flags.HasFlag(DocumentFlags.HasCounters);
                var hasAttachments = document.Flags.HasFlag(DocumentFlags.HasAttachments);

                if (hasRevisions)
                {

                }

                if (hasCounters)
                {

                }

                if (hasAttachments)
                {
                    if (metadata.Modifications == null)
                        metadata.Modifications = new DynamicJsonValue(metadata);

                    metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);

                    document.Flags &= ~DocumentFlags.HasAttachments;

                    // var actualAttachments = _storage.AttachmentsStorage.GetAttachmentsMetadataForDocument(_context, document.Id);
                }

                //document.Flags &= ~DocumentFlags.HasRevisions;
                //document.Flags &= ~DocumentFlags.HasCounters;
                //document.Flags &= ~DocumentFlags.HasAttachments;

                //metadata.Modifications = new DynamicJsonValue(metadata);
                //metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);


                //using (document.Data)

                if (document.Data.Modifications != null || metadata.Modifications != null)
                {
                    using (document.Data)
                        document.Data = _context.ReadObject(document.Data, document.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                _storage.Put(_context, id ?? document.Id, null, document.Data, document.LastModified.Ticks, flags: document.Flags);
            }
        }

        public void PutRevision(Document revision, string id = null)
        {
            using (_txManager.EnsureTransaction())
            {
                if (revision.Id == "doc/2")
                {

                }

                if (CanStoreRevision(revision) == false)
                {
                    _orphans.PutRevision(revision);
                    return;
                }

                var databaseChangeVector = _context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(_context);

                _storage.RevisionsStorage.Put(_context, id ?? revision.Id, revision.Data, revision.Flags, NonPersistentDocumentFlags.None, revision.ChangeVector, revision.LastModified.Ticks);

                _context.LastDatabaseChangeVector = ChangeVectorUtils.MergeVectors(databaseChangeVector, revision.ChangeVector);
            }
        }

        public void PutConflict(DocumentConflict conflict)
        {
            using (_txManager.EnsureTransaction())
            {
                _storage.ConflictsStorage.AddConflict(_context, conflict.Id, conflict.LastModified.Ticks, conflict.Doc, conflict.ChangeVector, conflict.Collection, conflict.Flags, NonPersistentDocumentFlags.FromSmuggler); // TODO arek - FromSmuggler
            }
        }

        public void PutCounter(CounterGroupDetail counterGroup)
        {
            using (_txManager.EnsureTransaction())
            {

            }
        }

        public void PutAttachment(FileStream stream, string hash, in long totalSize)
        {
            using (_txManager.EnsureTransaction())
            {

            }
        }

        private bool CanStoreRevision(Document revision)
        {
            if (revision.Flags.Contain(DocumentFlags.HasAttachments))
            {
                var metadata = revision.Data.GetMetadata();
                var metadataDictionary = new MetadataAsDictionary(metadata);

                var attachments = metadataDictionary.GetObjects(Constants.Documents.Metadata.Attachments);
                if (attachments != null)
                {
                    foreach (var attachment in attachments)
                    {
                        using (var hash = _context.GetLazyString(attachment.GetString(nameof(AttachmentName.Hash))))
                        {
                            if (_storage.AttachmentsStorage.AttachmentExists(_context, hash) == false)
                                return false; // revisions cannot be updated later so we need everything in place
                        }
                    }
                }
            }

            return true;
        }

        public void HandleOrphans()
        {
            using (_txManager.EnsureTransaction())
            {
                // attachments

                // revisions

                _orphans.HandleOrphanRevisions(_txManager);



                   // _txManager.MaybePulseTransaction();
            }
            
        }

        public IEnumerable<Document> GetDocumentsFromCollection(string collection)
        {
            return _storage.GetDocumentsFrom(_context, collection, 0, 0, int.MaxValue);
        }

        public void Dispose()
        {
            _txManager?.Dispose();
            _contextDisposal?.Dispose();
            _storage?.Dispose();
        }
    }
}
