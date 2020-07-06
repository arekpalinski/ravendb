namespace Voron.Recovery
{
    public static class Constants
    {
        public static class Orphans
        {
            public static class Revisions
            {
                public const string OriginalCollectionMetadataKey = "@original-collection";

                public const string CollectionName = "RecoveryOrphanRevisions";

                public static readonly string DocumentPrefixId = $"{CollectionName}-";

                public const string DocumentSuffixIdFormat = "-{0:D6}";

                public const int DocumentSuffixIdLength = 7;
            }

            public static class Attachments
            {
                public const string CollectionName = "RecoveryOrphanAttachments";

                public static readonly string DocumentPrefixId = $"{CollectionName}-";

                public const string AttachmentNamePrefix = "RecoveryAttachment-";

                public const string OrphanDocumentsPropertyName = "Documents";

                public const string NameProperty = "Name";

                public const string ContentTypeProperty = "ContentType";

                public const string OriginalDocIdProperty = "OriginalDocId";
            }
        }
    }
}
