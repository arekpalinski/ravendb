using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using FastTests.Utils;
using Raven.Client;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron.Bugs
{
    // The user-visible half of the DocumentCompare casing change (finding f-1). RevisionsStorage decides
    // whether to keep a revision by asking DocumentCompare whether the new document differs from the
    // stored one, so a metadata difference that DocumentCompare stops treating as significant stops
    // producing revisions. Same for PatchDocumentCommand, which discards a patch result that compares
    // Equal, and for the two conflict-resolution paths.
    public class MetadataCasingSkipsRevision(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Revisions, LicenseRequired = true)]
        [InlineData("@expires")]
        [InlineData("@Expires")]
        [InlineData("@refresh")]
        [InlineData("@Refresh")]
        public async Task ChangingAMetadataValueMustProduceARevisionRegardlessOfItsCasing(string metadataName)
        {
            using var store = GetDocumentStore();
            await RevisionsHelper.SetupRevisionsAsync(store);

            const string id = "users/1";

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Arava" };
                await session.StoreAsync(user, id);
                session.Advanced.GetMetadataFor(user)[metadataName] = "2030-01-01T00:00:00.0000000Z";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id);
                session.Advanced.GetMetadataFor(user)[metadataName] = "2040-01-01T00:00:00.0000000Z";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var revisions = await session.Advanced.Revisions.GetForAsync<User>(id);
                Assert.True(revisions.Count == 2,
                    $"changing the value of the metadata property '{metadataName}' produced {revisions.Count} revision(s), expected 2 - the change was treated as no change at all");
            }
        }

        // PatchDocumentCommand discards the patch result outright when it compares Equal to the original,
        // so the same casing change turns a patch into a silent no-op rather than only losing history
        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("@expires")]
        [InlineData("@Expires")]
        [InlineData("@refresh")]
        [InlineData("@Refresh")]
        public async Task PatchingAMetadataValueMustBeAppliedRegardlessOfItsCasing(string metadataName)
        {
            using var store = GetDocumentStore();

            const string id = "users/1";
            const string after = "2040-01-01T00:00:00.0000000Z";

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "Arava" };
                await session.StoreAsync(user, id);
                session.Advanced.GetMetadataFor(user)[metadataName] = "2030-01-01T00:00:00.0000000Z";
                await session.SaveChangesAsync();
            }

            await store.Operations.SendAsync(new PatchOperation(id, null, new PatchRequest
            {
                Script = "this['@metadata'][args.name] = args.value;",
                Values =
                {
                    ["name"] = metadataName,
                    ["value"] = after
                }
            }));

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id);
                var stored = session.Advanced.GetMetadataFor(user)[metadataName]?.ToString();
                Assert.True(stored == after,
                    $"a patch that set the metadata property '{metadataName}' to {after} left it at {stored ?? "<missing>"} - the patch result was discarded as no change");
            }
        }
    }
}
