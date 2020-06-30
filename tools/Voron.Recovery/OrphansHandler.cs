using System;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Voron.Recovery
{
    public class OrphansHandler
    {
        public const string OriginalCollectionMetadataKey = "@original-collection";

        private const string OrphanRevisionsCollectionName = "OrphanRevisions";
        private static readonly string OrphanRevisionsPrefixId = $"{OrphanRevisionsCollectionName}/";

        private readonly RecoveryStorage _recoveryStorage;

        public OrphansHandler(RecoveryStorage recoveryStorage)
        {
            _recoveryStorage = recoveryStorage;
        }

        public void PutRevision(Document revision)
        {
            UpdateMetadataToOrphanCollection(revision, OrphanRevisionsCollectionName);

            var orphanId = GetOrphanRevisionDocId(revision.Id);

            revision.Flags &= ~DocumentFlags.Revision; // let's put it as a regular document temporary

            _recoveryStorage.PutDocument(revision, orphanId, hasModifications: true);
        }

        private static void UpdateMetadataToOrphanCollection(Document revision, string orphanCollectionName)
        {
            DynamicJsonValue mutatedMetadata = null;
            string currentCollectionName = null;

            if (revision.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;

                metadata.TryGet(Constants.Documents.Metadata.Collection, out currentCollectionName);
            }

            revision.Data.Modifications = new DynamicJsonValue(revision.Data)
            {
                [Constants.Documents.Metadata.Key] = (object)metadata ?? (mutatedMetadata = new DynamicJsonValue())
            };

            mutatedMetadata[Constants.Documents.Metadata.Collection] = orphanCollectionName;

            if (string.IsNullOrEmpty(currentCollectionName) == false)
                mutatedMetadata[OriginalCollectionMetadataKey] = currentCollectionName;
        }

        private static string GetOrphanRevisionDocId(string revisionId) => $"{OrphanRevisionsPrefixId}{revisionId}/{Guid.NewGuid()}";
    }
}
