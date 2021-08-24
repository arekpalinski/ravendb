using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16726 : RavenTestBase
    {
        public RavenDB_16726(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Foo()
        {
            using (var store = GetDocumentStore())
            {
                new Products_ByCategory().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Category { Id = "categories/0", Name = "foo"});
                    session.Store(new Category { Id = "categories/1", Name = "bar"});

                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new Product { Category = $"categories/{i % 2}"});
                    }

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);
            }
        }

        private class Products_ByCategory : AbstractIndexCreationTask<Product>
        {
            public Products_ByCategory()
            {
                Map = products => from product in products
                    let category = LoadDocument<Category>(product.Category)
                    select new
                    {
                        CategoryId = category.Name
                    };
            }
        }
    }
}
