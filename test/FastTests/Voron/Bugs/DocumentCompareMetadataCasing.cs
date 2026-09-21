using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // DocumentCompare.IsSignificantMetadataProperty decides whether a metadata property difference makes
    // two documents differ. It used to match the well-known names with StringComparison.OrdinalIgnoreCase;
    // it now matches raw bytes through IsEqualConstant. These tests pin what that changes, so the flip
    // shows up as a test result rather than as a code reading.
    public class DocumentCompareMetadataCasing : NoDisposalNeeded
    {
        public DocumentCompareMetadataCasing(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("@expires")]  // canonical: significant on both sides
        [InlineData("@Expires")]  // non-canonical casing: significant before, ignored now
        [InlineData("@EXPIRES")]
        [InlineData("@refresh")]
        [InlineData("@Refresh")]
        [InlineData("@collection")]
        [InlineData("@Collection")]
        public void MetadataPropertyDifferenceIsSignificantRegardlessOfItsCasing(string name)
        {
            using var context = JsonOperationContext.ShortTermSingleUse();

            using var a = Build(context, name, "2030-01-01T00:00:00.0000000Z");
            using var b = Build(context, name, "2040-01-01T00:00:00.0000000Z");

            var result = DocumentCompare.IsEqualTo(a, b, DocumentCompare.DocumentCompareOptions.Default);

            Assert.True(result == DocumentCompareResult.NotEqual,
                $"two documents whose only difference is the value of the metadata property '{name}' compared as {result}");
        }

        private static BlittableJsonReaderObject Build(JsonOperationContext context, string metadataName, string value)
        {
            var djv = new DynamicJsonValue
            {
                ["Name"] = "Arava",
                [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Collection] = "Users",
                    [metadataName] = value
                }
            };

            return context.ReadObject(djv, "doc", BlittableJsonDocumentBuilder.UsageMode.None);
        }
    }
}
