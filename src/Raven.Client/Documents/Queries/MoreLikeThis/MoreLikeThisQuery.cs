using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisQuery : MoreLikeThisQuery<Dictionary<string, object>>
    {
    }

    public abstract class MoreLikeThisQuery<T> : IIndexQuery
        where T : class
    {
        private int _pageSize = int.MaxValue;

        protected MoreLikeThisQuery()
        {
            MapGroupFields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        public const int DefaultMaximumNumberOfTokensParsed = 5000;
        public const int DefaultMinimumTermFrequency = 2;
        public const int DefaultMinimumDocumentFrequency = 5;
        public const int DefaultMaximumDocumentFrequency = int.MaxValue;
        public const bool DefaultBoost = false;
        public const float DefaultBoostFactor = 1;
        public const int DefaultMinimumWordLength = 0;
        public const int DefaultMaximumWordLength = 0;
        public const int DefaultMaximumQueryTerms = 25;

        /// <summary>
        /// Ignore terms with less than this frequency in the source doc. Default is 2.
        /// </summary>
        public int? MinimumTermFrequency { get; set; }

        /// <summary>
        /// Ignore words which do not occur in at least this many documents. Default is 5.
        /// </summary>
        public int? MinimumDocumentFrequency { get; set; }

        /// <summary>
        /// Ignore words which occur in more than this many documents. Default is Int32.MaxValue.
        /// </summary>
        public int? MaximumDocumentFrequency { get; set; }

        /// <summary>
        /// Ignore words which occur in more than this percentage of documents.
        /// </summary>
        public int? MaximumDocumentFrequencyPercentage { get; set; }

        /// <summary>
        /// Boost terms in query based on score. Default is false.
        /// </summary>
        public bool? Boost { get; set; }

        /// <summary>
        /// Boost factor when boosting based on score. Default is 1.
        /// </summary>
        public float? BoostFactor { get; set; }

        /// <summary>
        /// Ignore words less than this length or if 0 then this has no effect. Default is 0.
        /// </summary>
        public int? MinimumWordLength { get; set; }

        /// <summary>
        /// Ignore words greater than this length or if 0 then this has no effect. Default is 0.
        /// </summary>
        public int? MaximumWordLength { get; set; }

        /// <summary>
        /// Return a Query with no more than this many terms. Default is 25.
        /// </summary> 
        public int? MaximumQueryTerms { get; set; }

        /// <summary>
        /// The maximum number of tokens to parse in each example doc field that is not stored with TermVector support. Default is 5000.
        /// </summary>
        public int? MaximumNumberOfTokensParsed { get; set; }

        /// <summary>
        /// The document id containing the custom stop words
        /// </summary>
        public string StopWordsDocumentId { get; set; }

        /// <summary>
        /// The fields to compare
        /// </summary>
        public string[] Fields { get; set; }

        /// <summary>
        /// The document id to use as the basis for comparison
        /// </summary>
        public string DocumentId { get; set; }

        /// <summary>
        /// The name of the index to use for this operation
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// An additional query that the matching documents need to also
        /// match to be returned. 
        /// </summary>
        public string AdditionalQuery { get; set; }

        /// <summary>
        /// Values for the mapping group fields to use as the basis for comparison
        /// </summary>
        public Dictionary<string, string> MapGroupFields { get; set; }

        /// <summary>
        /// Transformer to use on the query results.
        /// </summary>
        public string Transformer { get; set; }

        /// <summary>
        /// Array of paths under which document Ids can be found. All found documents will be returned with the query results.
        /// </summary>
        public string[] Includes { get; set; }

        /// <summary>
        /// Parameters that will be passed to transformer.
        /// </summary>
        public T TransformerParameters { get; set; }

        /// <summary>
        /// Maximum number of records that will be retrieved.
        /// </summary>
        public int PageSize
        {
            get => _pageSize;
            set
            {
                _pageSize = value;
                PageSizeSet = true;
            }
        }

        /// <summary>
        /// Whether the page size was explicitly set or still at its default value
        /// </summary>
        protected internal bool PageSizeSet { get; private set; }
    }
}
