using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Elastic;

namespace Raven.Server.Documents.ETL.Providers.Enumerators
{
    public class DocumentsToElasticsearchEtlItems : IEnumerator<ElasticItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Document> _docs;

        public DocumentsToElasticsearchEtlItems(IEnumerator<Document> docs, string collection)
        {
            _docs = docs;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_docs.MoveNext() == false)
                return false;

            Current = new ElasticItem(_docs.Current, _collection);

            return true;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public ElasticItem Current { get; private set; }
    }
}
