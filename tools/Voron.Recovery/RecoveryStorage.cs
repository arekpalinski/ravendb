using System;
using System.Collections.Generic;
using System.IO;
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
using Voron.Recovery.Orphans;

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
                if (string.IsNullOrEmpty(id))
                    id = document.Id;

                var metadata = document.Data.GetMetadata();
                if (metadata == null)
                    throw new Exception($"No metadata for {id}, cannot recover this document");

                var metadataDictionary = new MetadataAsDictionary(metadata);

                if (document.Flags.HasFlag(DocumentFlags.Revision))
                    throw new InvalidOperationException($"Cannot put a revision directly as a document. Revision id: {id}");

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
                    if (AllAttachmentsExist(document) == false)
                    {
                        var attachments = metadataDictionary.GetObjects(Raven.Client.Constants.Documents.Metadata.Attachments);

                        _orphans.Attachments.StoreOrphanAttachments(id, attachments);

                        if (metadata.Modifications == null)
                            metadata.Modifications = new DynamicJsonValue(metadata);

                        metadata.Modifications.Remove(Raven.Client.Constants.Documents.Metadata.Attachments);

                        document.Flags &= ~DocumentFlags.HasAttachments;
                    }
                    else
                    {
                        // TODO arek
                    }

                    //var actualAttachments = _storage.AttachmentsStorage.GetAttachmentsMetadataForDocument(_context, document.Id);
                }

                //document.Flags &= ~DocumentFlags.HasRevisions;
                //document.Flags &= ~DocumentFlags.HasCounters;
                //document.Flags &= ~DocumentFlags.HasAttachments;

                //metadata.Modifications = new DynamicJsonValue(metadata);
                //metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);

                EnsureModificationsApplied(document);

                _storage.Put(_context, id, null, document.Data, document.LastModified.Ticks, flags: document.Flags);
            }
        }

        public void PutDocument(string id, DynamicJsonValue document)
        {
            using (var doc = _context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                _storage.Put(_context, id, null, doc);
            }
        }


        private void EnsureModificationsApplied(Document document)
        {
            var metadata = document.Data.GetMetadata();

            if (document.Data.Modifications != null || metadata.Modifications != null)
            {
                using (document.Data)
                    document.Data = _context.ReadObject(document.Data, document.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
        }

        public void PutRevision(Document revision, string id = null)
        {
            using (_txManager.EnsureTransaction())
            {
                if (AllAttachmentsExist(revision) == false)
                {
                    _orphans.Revisions.Put(revision);
                    return;
                }

                var databaseChangeVector = _context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(_context);

                EnsureModificationsApplied(revision);

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

        public void PutAttachment(Stream stream, string hash, long totalSize)
        {
            using (_txManager.EnsureTransaction())
            {
                _orphans.Attachments.Put(stream, hash, totalSize);

                // check which documents already seen for this attachment and attach it to them

                var orphanDocs = _orphans.Attachments.GetOrphanDocumentsForAttachment(hash);

                foreach (var item in orphanDocs)
                {
                    
                }

                // TODO arek - delete items from array
            }
        }

        public void PutAttachment(string documentId, string name, Stream stream, string hash)
        {
            using (_txManager.EnsureTransaction())
            {
                _storage.AttachmentsStorage.PutAttachment(_context, documentId, name, string.Empty, hash, null, stream);
            }
        }

        public bool AttachmentExists(string hash)
        {
            using (_txManager.EnsureTransaction())
            {

                using (var hashLsv = _context.GetLazyString(hash))
                {
                    return _storage.AttachmentsStorage.AttachmentExists(_context, hashLsv);
                }
            }
        }

        private bool AllAttachmentsExist(Document doc)
        {
            if (doc.Flags.Contain(DocumentFlags.HasAttachments))
            {
                var metadata = doc.Data.GetMetadata();
                var metadataDictionary = new MetadataAsDictionary(metadata);

                var attachments = metadataDictionary.GetObjects(Raven.Client.Constants.Documents.Metadata.Attachments);
                if (attachments != null)
                {
                    foreach (var attachment in attachments)
                    {
                        if (AttachmentExists(attachment.GetString(nameof(AttachmentName.Hash))) == false)
                            return false; // revisions cannot be updated later so we need everything in place
                    }
                }
            }

            return true;
        }

        public void ProcessOrphans()
        {
            _txManager.PulseTransaction(); // this is to update DocumentsStorage._collectionsCache which is updated only on transaction commit, so we'll see orphan collections

            using (_txManager.EnsureTransaction())
            {
                // attachments

                // revisions

                _orphans.Revisions.ProcessExistingOrphans(_txManager);



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

        public void Delete(string id)
        {
            using (_txManager.EnsureTransaction())
            {
                _storage.Delete(_context, id, null);
            }
        }
    }
}
