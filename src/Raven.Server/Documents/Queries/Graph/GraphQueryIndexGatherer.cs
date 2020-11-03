using System.Collections.Generic;
using System.Threading.Tasks;
using NCrontab.Advanced.Extensions;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryIndexNamesGatherer: QueryPlanVisitor
    {        
        public List<string> Indexes { get; } = new List<string>();
        
        public List<QueryQueryStep> QueryStepsWithoutExplicitIndex { get; } = new List<QueryQueryStep>();

        public override Task VisitQueryQueryStepAsync(QueryQueryStep qqs)
        {
            var name = qqs.GetIndexName;
            if (name.IsNullOrWhiteSpace() == false)
            {
                Indexes.Add(name);
            }
            else
            {
                QueryStepsWithoutExplicitIndex.Add(qqs);
            }

            return Task.CompletedTask;
        }
    }
}
