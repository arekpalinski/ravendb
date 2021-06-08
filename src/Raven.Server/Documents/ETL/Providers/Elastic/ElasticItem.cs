namespace Raven.Server.Documents.ETL.Providers.Elastic
{
    public class ElasticItem : ExtractedItem
    {
        public ElasticItem(ElasticItem item)
        {
            Etag = item.Etag;
            DocumentId = item.DocumentId;
            Document = item.Document;
            IsDelete = item.IsDelete;
            Collection = item.Collection;
            ChangeVector = item.ChangeVector;
        }

        public ElasticItem(Document document, string collection) : base(document, collection, EtlItemType.Document)
        {
        }

        public ElasticItem(Tombstone tombstone, string collection) : base(tombstone, collection, EtlItemType.Document)
        {
        }
        
        public ElasticProperty Property { get; set; }
    }
}
