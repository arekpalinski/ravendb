using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // Guards the MultiGet half of finding f-2. AbstractMultiGetHandlerProcessorForPost used to skip a
    // header whose value was not a string (headers.TryGet(header, out string value) == false -> continue)
    // and now calls BlittableJsonReaderObject.ConvertType unconditionally (:236). The concern was that
    // ConvertType throws FormatException on a value it cannot convert, on a path fed straight from the
    // request body.
    // Measured: it does not, for T = string. ConvertType's default branch ends at
    // Convert.ChangeType(result, typeof(string)), which routes through Convert.ToString and falls back to
    // the object's own ToString, so an object or an array is stringified rather than rejected. The change
    // is real - the header is now SET to the JSON text instead of being skipped - but it is not a server
    // error. Keeping the test so that stays true.
    public class MultiGetWrongTypedHeader(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("\"a string\"", "a string value is fine on both sides")]
        [InlineData("{\"nested\":1}", "an object where a string is expected")]
        [InlineData("[1,2,3]", "an array where a string is expected")]
        public async Task AWrongTypedHeaderValueMustNotProduceAServerError(string headerValueJson, string what)
        {
            using var store = GetDocumentStore();

            var body = $$"""
                {
                  "Requests": [
                    {
                      "Url": "/docs",
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

            // multi_get answers 200 at the envelope level and carries per-request status inside, so the
            // body is where a failure shows up
            Assert.True(response.StatusCode == HttpStatusCode.OK,
                $"multi_get with {what} answered {(int)response.StatusCode} {response.StatusCode}: {content}");

            Assert.False(content.Contains("FormatException") || content.Contains("\"StatusCode\":500"),
                $"multi_get with {what} returned a failing sub-result: {content}");
        }
    }
}
