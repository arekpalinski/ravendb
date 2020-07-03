using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Voron.Recovery
{
    public class OrphansHandler
    {
        public const string OriginalCollectionMetadataKey = "@original-collection";

        private const string OrphanRevisionsCollectionName = "OrphanRevisions";
        private static readonly string OrphanRevisionsPrefixId = $"{OrphanRevisionsCollectionName}-";
        private const int OrphanRevisionsSuffixIdLength = 7;

        private long _orphanedRevisionsCounter;

        private readonly RecoveryStorage _recoveryStorage;

        public OrphansHandler(RecoveryStorage recoveryStorage)
        {
            _recoveryStorage = recoveryStorage;
        }

        public void PutRevision(Document revision)
        {
            var updated = UpdateCollectionMetadata(revision, OrphanRevisionsCollectionName, out var originalCollection);

            if (string.IsNullOrEmpty(originalCollection) == false)
                updated[OriginalCollectionMetadataKey] = originalCollection;

            var orphanId = GetOrphanRevisionDocId(revision.Id);

            revision.Flags &= ~DocumentFlags.Revision; // let's put it as a regular document temporary

            _recoveryStorage.PutDocument(revision, orphanId);
        }

        public void HandleOrphanRevisions(RecoveryTransactionManager txManager)
        {
            var txScope = txManager.EnsureTransaction();

            try
            {
                do
                {
                    var didWork = false;

                    var orphanRevisions = _recoveryStorage.GetDocumentsFromCollection(OrphanRevisionsCollectionName);

                    foreach (var doc in orphanRevisions)
                    {
                        didWork = true;

                        if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                        {
                            if (metadata.TryGet(OriginalCollectionMetadataKey, out LazyStringValue originalCollection))
                            {
                                var updated = UpdateCollectionMetadata(doc, originalCollection.ToString(), out _);

                                updated.Remove(OriginalCollectionMetadataKey);
                            }
                        }

                        string orphanDocId = doc.Id.ToString();

                        var originalDocId = orphanDocId.Substring(OrphanRevisionsPrefixId.Length, orphanDocId.Length - OrphanRevisionsPrefixId.Length - OrphanRevisionsSuffixIdLength);

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

            if (revision.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;

                metadata.TryGet(Constants.Documents.Metadata.Collection, out originalCollectionName);
            }
            else
            {
                mutatedMetadata = new DynamicJsonValue();
                originalCollectionName = null;
            }

            revision.Data.Modifications = new DynamicJsonValue(revision.Data)
            {
                [Constants.Documents.Metadata.Key] = (object)metadata ?? mutatedMetadata
            };

            mutatedMetadata[Constants.Documents.Metadata.Collection] = collectionName;

            return mutatedMetadata;
        }

        private string GetOrphanRevisionDocId(string revisionId) => $"{OrphanRevisionsPrefixId}{revisionId}{GetOrphanRevisionsSuffixId()}";

        private string GetOrphanRevisionsSuffixId()
        {
            var result = $"-{Interlocked.Increment(ref _orphanedRevisionsCounter):D6}";

            Debug.Assert(result.Length == OrphanRevisionsSuffixIdLength, "result.Length == OrphanRevisionsSuffixIdLength");

            return result;
        }
    }
}
