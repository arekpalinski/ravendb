using System.Collections.Generic;
using System.IO;
using System.Threading;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Voron.Recovery.Orphans
{
    public class OrphanAttachmentsHandler
    {
        private long _orphanedAttachmentsCounter;

        private readonly RecoveryStorage _recoveryStorage;

        public OrphanAttachmentsHandler(RecoveryStorage recoveryStorage)
        {
            _recoveryStorage = recoveryStorage;
        }

        public string GetOrphanDocId(string hash) => $"{Constants.Orphans.Attachments.DocumentPrefixId}{hash}";

        private string GetOrphanAttachmentName()
        {
            var result = $"{Constants.Orphans.Attachments.AttachmentNamePrefix}{Interlocked.Increment(ref _orphanedAttachmentsCounter)}";

            return result;
        }

        public void Put(Stream stream, string hash, long totalSize)
        {
            var orphanAttachmentDocId = GetOrphanDocId(hash);

            if (_recoveryStorage.AttachmentExists(hash))
                return;

            PutOrphanDocument(orphanAttachmentDocId);

            var attachmentName = GetOrphanAttachmentName();

            _recoveryStorage.PutAttachment(orphanAttachmentDocId, attachmentName, stream, hash);
        }

        private void PutOrphanDocument(string id)
        {
            var orphanDoc = new DynamicJsonValue
            {
                [Constants.Orphans.Attachments.OrphanDocumentsPropertyName] = new DynamicJsonArray(),
                [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = Constants.Orphans.Attachments.CollectionName
                }
            };

            _recoveryStorage.PutDocument(id, orphanDoc);
        }

        public void StoreOrphanAttachments(string documentId, IMetadataDictionary[] attachments)
        {
            foreach (var attachment in attachments)
            {
                var hash = attachment.GetString(nameof(AttachmentName.Hash));
                var attachmentName = attachment.GetString(nameof(AttachmentName.Name));
                var contentType = attachment.GetString(nameof(AttachmentName.ContentType));

                if (string.IsNullOrEmpty(hash) || string.IsNullOrEmpty(attachmentName))
                {
                    // TODO: Log($"Document {document.Id} has attachment flag set with empty hash / name");
                    continue;
                }

                var orphanDocId = GetOrphanDocId(hash);

                var orphanAttachmentDoc = _recoveryStorage.Get(orphanDocId);

                if (orphanAttachmentDoc == null)
                {
                    PutOrphanDocument(orphanDocId);
                    orphanAttachmentDoc = _recoveryStorage.Get(orphanDocId);
                }

                var docsArray = new DynamicJsonArray();

                if (orphanAttachmentDoc.Data.TryGet(Constants.Orphans.Attachments.OrphanDocumentsPropertyName, out BlittableJsonReaderArray docs))
                {
                    docs.Modifications = docsArray = new DynamicJsonArray()
                }

                docsArray.Add(new DynamicJsonValue
                {
                    [nameof(Constants.Orphans.Attachments.NameProperty)] = attachmentName,
                    [nameof(Constants.Orphans.Attachments.ContentTypeProperty)] = contentType,
                    [nameof(Constants.Orphans.Attachments.OriginalDocIdProperty)] = documentId
                });

                _recoveryStorage.PutDocument(orphanAttachmentDoc, orphanDocId);
            }
        }

        public List<(string AttachmentName, string ContentType, string DocumentId)> GetOrphanDocumentsForAttachment(string hash)
        {
            var orphanDocId = GetOrphanDocId(hash);

            var orphanAttachmentDoc = _recoveryStorage.Get(orphanDocId);

            var result = new List<(string AttachmentName, string ContentType, string DocumentId)>();

            if (orphanAttachmentDoc.Data.TryGet(Constants.Orphans.Attachments.OrphanDocumentsPropertyName, out BlittableJsonReaderArray docs) == false)
                return result;

            foreach (var item in docs)
            {
                
            }

            return result;
        }
    }
}
