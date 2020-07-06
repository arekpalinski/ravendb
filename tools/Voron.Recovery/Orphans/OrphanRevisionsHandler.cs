using System.Diagnostics;
using System.Threading;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Voron.Recovery.Orphans
{
    public class OrphanRevisionsHandler
    {
        private long _orphanedRevisionsCounter;

        private readonly RecoveryStorage _recoveryStorage;

        public OrphanRevisionsHandler(RecoveryStorage recoveryStorage)
        {
            _recoveryStorage = recoveryStorage;
        }

        public void Put(Document revision)
        {
            var updated = UpdateCollectionMetadata(revision, Constants.Orphans.Revisions.CollectionName, out var originalCollection);

            if (string.IsNullOrEmpty(originalCollection) == false)
                updated[Constants.Orphans.Revisions.OriginalCollectionMetadataKey] = originalCollection;

            var orphanId = GetOrphanRevisionDocId(revision.Id);

            revision.Flags &= ~DocumentFlags.Revision; // let's put it as a regular document temporary

            _recoveryStorage.PutDocument(revision, orphanId);
        }

        public void ProcessExistingOrphans(RecoveryTransactionManager txManager)
        {
            var txScope = txManager.EnsureTransaction();

            try
            {
                do
                {
                    var didWork = false;

                    var orphanRevisions = _recoveryStorage.GetDocumentsFromCollection(Constants.Orphans.Revisions.CollectionName);

                    foreach (var doc in orphanRevisions)
                    {
                        didWork = true;

                        if (doc.Data.TryGet(Raven.Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                        {
                            if (metadata.TryGet(Constants.Orphans.Revisions.OriginalCollectionMetadataKey, out LazyStringValue originalCollection))
                            {
                                var updated = UpdateCollectionMetadata(doc, originalCollection.ToString(), out _);

                                updated.Remove(Constants.Orphans.Revisions.OriginalCollectionMetadataKey);
                            }
                        }

                        string orphanDocId = doc.Id.ToString();

                        var originalDocId = orphanDocId.Substring(Constants.Orphans.Revisions.DocumentPrefixId.Length, orphanDocId.Length - Constants.Orphans.Revisions.DocumentPrefixId.Length - Constants.Orphans.Revisions.DocumentSuffixIdLength);

                        _recoveryStorage.PutRevision(doc, originalDocId);

                        _recoveryStorage.Delete(orphanDocId);

                        break;
                    }

                    if (txManager.MaybePulseTransaction())
                        txScope = txManager.EnsureTransaction();

                    if (didWork == false)
                        break;

                } while (true);
            }
            finally
            {
                txScope.Dispose();
            }
        }

        private static DynamicJsonValue UpdateCollectionMetadata(Document revision, string collectionName, out string originalCollectionName)
        {
            DynamicJsonValue mutatedMetadata;

            if (revision.Data.TryGet(Raven.Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;

                metadata.TryGet(Raven.Client.Constants.Documents.Metadata.Collection, out originalCollectionName);
            }
            else
            {
                mutatedMetadata = new DynamicJsonValue();
                originalCollectionName = null;
            }

            revision.Data.Modifications = new DynamicJsonValue(revision.Data)
            {
                [Raven.Client.Constants.Documents.Metadata.Key] = (object)metadata ?? mutatedMetadata
            };

            mutatedMetadata[Raven.Client.Constants.Documents.Metadata.Collection] = collectionName;

            return mutatedMetadata;
        }

        private string GetOrphanRevisionDocId(string revisionId) => $"{Constants.Orphans.Revisions.DocumentPrefixId}{revisionId}{GetOrphanRevisionsSuffixId()}";

        private string GetOrphanRevisionsSuffixId()
        {
            var result = string.Format(Constants.Orphans.Revisions.DocumentSuffixIdFormat, Interlocked.Increment(ref _orphanedRevisionsCounter));

            Debug.Assert(result.Length == Constants.Orphans.Revisions.DocumentSuffixIdLength, "result.Length == Constants.Orphans.Revisions.DocumentSuffixIdLength");

            return result;
        }
    }
}
