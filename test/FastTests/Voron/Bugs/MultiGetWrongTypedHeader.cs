using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // Finding f-2, MultiGet half. AbstractMultiGetHandlerProcessorForPost used to skip a header whose
    // value was not a string (headers.TryGet(header, out string value) == false -> continue) and now
    // calls BlittableJsonReaderObject.ConvertType unconditionally (:236). ConvertType rejects an
    // object-or-array/scalar mismatch with a FormatException before it ever reaches the lenient
    // Convert.ChangeType fallback, and FormatException is not in RavenServerStartup's
    // exception-to-status map, so the whole multi_get request answers 500.
    //
    // Note on the Url below: multi_get resolves each sub-request through the global router, so the Url
    // must carry the /databases/<name> prefix the client sends. Without it every sub-request takes
    // HandleNoRoute and returns before PrepareHttpContextAsync ever reads the headers - an earlier
    // version of this test used "/docs" and therefore never exercised the code it was guarding.
    public class MultiGetWrongTypedHeader(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("\"a string\"", "a string value is fine on both sides")]
        [InlineData("12345", "a number where a string is expected")]
        [InlineData("true", "a boolean where a string is expected")]
        [InlineData("{\"nested\":1}", "an object where a string is expected")]
        [InlineData("{}", "an empty object where a string is expected")]
        [InlineData("[1,2,3]", "an array where a string is expected")]
        [InlineData("[]", "an empty array where a string is expected")]
        public async Task AWrongTypedHeaderValueMustNotProduceAServerError(string headerValueJson, string what)
        {
            using var store = GetDocumentStore();

            var body = $$"""
                {
                  "Requests": [
                    {
                      "Url": "/databases/{{store.Database}}/docs",
                      "Query": "?id=users/1",
                      "Method": "GET",
                      "Headers": { "X-Probe": {{headerValueJson}} }
                    }
                  ]
                }
                """;

            using var client = new HttpClient();
            var url = $"{store.Urls[0]}/databases/{store.Database}/multi_get";
            using var response = await client.PostAsync(url, new StringContent(body, Encoding.UTF8, "application/json"));
            var content = await response.Content.ReadAsStringAsync();

            // the sub-request must actually have been routed, otherwise the header code never ran and
            // this test would pass without proving anything
            Assert.DoesNotContain("There is no handler for path", content);

            // multi_get answers 200 at the envelope level and carries per-request status inside, so a
            // failure can show up either place
            Assert.True(response.StatusCode == HttpStatusCode.OK,
                $"multi_get with {what} answered {(int)response.StatusCode} {response.StatusCode}: {content}");

            Assert.False(content.Contains("FormatException") || content.Contains("\"StatusCode\":500"),
                $"multi_get with {what} returned a failing sub-result: {content}");
        }
    }
}
