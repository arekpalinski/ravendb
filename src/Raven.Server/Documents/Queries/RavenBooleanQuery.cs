using System;
using System.Collections.Generic;
using Lucene.Net.Search;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Queries
{
    public class RavenBooleanQuery : BooleanQuery
    {
        private readonly OperatorType _operator;
        private bool _canBeTermsMatchQuery = false;
        private string _termsMatchQueryField = null;

        public RavenBooleanQuery(OperatorType @operator)
        {
            _operator = @operator;

            if (_operator == OperatorType.Or)
                _canBeTermsMatchQuery = true;
        }

        public bool CanConvertToTermsMatchQuery => _canBeTermsMatchQuery;

        public TermsMatchQuery ConvertToTermsMatchQuery()
        {
            var termQueryMatches = new List<string>();

            foreach (var booleanClause in GetClauses())
            {
                var tq = (TermQuery)booleanClause.Query;

                termQueryMatches.Add(tq.Term.Text);
            }

            return new TermsMatchQuery(_termsMatchQueryField, termQueryMatches);
        }

        public RavenBooleanQuery(Query left, Query right, OperatorType @operator)
        {
            _operator = @operator;

            switch (@operator)
            {
                case OperatorType.And:
                    Add(new BooleanClause(left, Occur.MUST));
                    TryAnd(right);
                    break;
                case OperatorType.Or:
                    Add(new BooleanClause(left, Occur.SHOULD));
                    TryOr(right);

                    if (CheckIfCanBeTermsMatchQuery(left, right, out _termsMatchQueryField))
                        _canBeTermsMatchQuery = true;

                    break;
                default:
                    ThrowInvalidOperatorType(@operator);
                    break;
            }
        }

        public bool TryAnd(Query right)
        {
            if (_operator == OperatorType.And)
            {
                Add(right, Occur.MUST);
                return true;
            }

            return false;
        }

        public void And(Query left, Query right)
        {
            if (_operator != OperatorType.And)
                ThrowInvalidOperator(OperatorType.And);

            Add(left, Occur.MUST);
            Add(right, Occur.MUST);
        }

        public bool TryOr(Query right)
        {
            if (_canBeTermsMatchQuery)
            {
                if (CheckIfCanBeTermsMatchQuery(_termsMatchQueryField, right) == false)
                {
                    _canBeTermsMatchQuery = false;
                    _termsMatchQueryField = null;
                }
            }

            if (_operator == OperatorType.Or)
            {
                Add(right, Occur.SHOULD);
                return true;
            }

            return false;
        }

        public void Or(Query left, Query right)
        {
            if (_canBeTermsMatchQuery)
            {
                if (CheckIfCanBeTermsMatchQuery(left, right, out _termsMatchQueryField) == false)
                {
                    _canBeTermsMatchQuery = false;
                    _termsMatchQueryField = null;
                }
            }

            if (_operator != OperatorType.Or)
                ThrowInvalidOperator(OperatorType.Or);
            
            Add(left, Occur.SHOULD);
            Add(right, Occur.SHOULD);
        }

        private static bool CheckIfCanBeTermsMatchQuery(Query left, Query right, out string fieldName)
        {
            if (left is TermQuery ltq && right is TermQuery rtq)
            {
                if (ltq.Term.Field.Equals(rtq.Term.Field, StringComparison.Ordinal) == false)
                {
                    fieldName = null;
                    return false;
                }

                fieldName = ltq.Term.Field;
                return true;
            }

            fieldName = null;
            return false;
        }

        private static bool CheckIfCanBeTermsMatchQuery(string fieldName, Query right)
        {
            if (right is TermQuery rtq)
            {
                if (fieldName.Equals(rtq.Term.Field, StringComparison.Ordinal) == false)
                    return false;
                
                return true;
            }
            
            return false;
        }

        private void ThrowInvalidOperator(OperatorType @operator)
        {
            throw new InvalidOperationException($"Cannot '{@operator}' query clause because current operator is {_operator}");
        }

        private static void ThrowInvalidOperatorType(OperatorType operatorType)
        {
            throw new ArgumentException($"{nameof(RavenBooleanQuery)} doesn't handle '{operatorType}' operator");
        }
    }
}
