using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryDetailedReporter : QueryPlanVisitor
    {
        private AsyncBlittableJsonTextWriter _writer;
        private DocumentsOperationContext _ctx;

        public GraphQueryDetailedReporter(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext ctx)
        {
            _writer = writer;
            _ctx = ctx;
        }

        public override async Task VisitQueryQueryStepAsync(QueryQueryStep qqs)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("QueryQueryStep");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Query");
            await _writer.WriteStringAsync(qqs.Query.ToString());
            await _writer.WriteCommaAsync();            
            await WriteIntermidiateResults(qqs.IntermediateResults);
            await _writer.WriteEndObjectAsync();
        }

        private async Task WriteIntermidiateResults(List<GraphQueryRunner.Match> matches)
        {
            await _writer.WritePropertyNameAsync("Results");
            await _writer.WriteStartArrayAsync();
            var first = true;
            foreach (var match in matches)
            {
                if (first == false)
                {
                    await _writer.WriteCommaAsync();
                }

                first = false;
                var djv = new DynamicJsonValue();
                match.PopulateVertices(djv);
                await _writer.WriteObjectAsync(_ctx.ReadObject(djv, null));
            }

            await _writer.WriteEndArrayAsync();
        }

        public override async Task VisitEdgeQueryStepAsync(EdgeQueryStep eqs)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("EdgeQueryStep");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Left");
            await VisitAsync(eqs.Left);
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Right");
            await VisitAsync(eqs.Right);
            await _writer.WriteEndObjectAsync();
        }

        public override async Task VisitCollectionDestinationQueryStepAsync(CollectionDestinationQueryStep cdqs)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("CollectionDestinationQueryStep");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Collection");
            await _writer.WriteStringAsync(cdqs.CollectionName);
            await _writer.WriteCommaAsync();
            await WriteIntermidiateResults(cdqs.IntermediateResults);
            await _writer.WriteEndObjectAsync();
        }

        public override async Task VisitIntersectionQueryStepExceptAsync(IntersectionQueryStep<Except> iqse)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("IntersectionQueryStep<Except>");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Left");
            await VisitAsync(iqse.Left);
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Right");
            await VisitAsync(iqse.Right);
            await _writer.WriteCommaAsync();
            await WriteIntermidiateResults(iqse.IntermediateResults);
            await _writer.WriteEndObjectAsync();
        }

        public override async Task VisitIntersectionQueryStepUnionAsync(IntersectionQueryStep<Union> iqsu)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("IntersectionQueryStep<Except>");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Left");
            await VisitAsync(iqsu.Left);
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Right");
            await VisitAsync(iqsu.Right);
            await _writer.WriteCommaAsync();
            await WriteIntermidiateResults(iqsu.IntermediateResults);
            await _writer.WriteEndObjectAsync();
        }

        public override async Task VisitIntersectionQueryStepIntersectionAsync(IntersectionQueryStep<Intersection> iqsi)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("IntersectionQueryStep<Except>");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Left");
            await VisitAsync(iqsi.Left);
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Right");
            await VisitAsync(iqsi.Right);
            await _writer.WriteCommaAsync();
            await WriteIntermidiateResults(iqsi.IntermediateResults);
            await _writer.WriteEndObjectAsync();
        }

        public override async Task VisitRecursionQueryStepAsync(RecursionQueryStep rqs)
        {
            await _writer.WriteStartObjectAsync();
            await _writer.WritePropertyNameAsync("Type");
            await _writer.WriteStringAsync("RecursionQueryStep");
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Left");
            await VisitAsync(rqs.Left);
            await _writer.WriteCommaAsync();
            await _writer.WritePropertyNameAsync("Steps");
            await _writer.WriteStartArrayAsync();
            var first = true;
            foreach (var step in rqs.Steps)
            {
                if (first == false)
                {
                    await _writer.WriteCommaAsync();
                }

                first = false;
                await VisitAsync(step.Right);
            }
            await _writer.WriteEndArrayAsync();
            await _writer.WriteCommaAsync();
            await VisitAsync(rqs.GetNextStep());
            await WriteIntermidiateResults(rqs.IntermediateResults);
            await _writer.WriteEndObjectAsync();
        }

        public override async Task VisitEdgeMatcherAsync(EdgeQueryStep.EdgeMatcher em)
        {
            await _writer.WritePropertyNameAsync("Next");
            await VisitAsync(em._parent.Right);
            await _writer.WriteCommaAsync();
        }
    }
}
