using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Elastic.Enumerators
{
    public class TombstonesToElasticItems : IEnumerator<ElasticItem>
    {
        private readonly string _collection;
        private readonly IEnumerator<Tombstone> _tombstones;

        public TombstonesToElasticItems(IEnumerator<Tombstone> tombstones, string collection)
        {
            _tombstones = tombstones;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new ElasticItem(_tombstones.Current, _collection) {Filtered = Filter()};

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

        private bool Filter()
        {
            return _tombstones.Current.Type != Tombstone.TombstoneType.Document;
        }
    }
}
