// -----------------------------------------------------------------------
//  <copyright file="LazyEncoding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.MailingList
{
    public class LazyEncoding : RavenTestBase
    {
        private class User
        {
            public string Name { get; set; }
            public bool Admin { get; set; }
        }

        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users =>
                    from u in users
                    select new { u.Name, u.Admin };
            }
        }

        [Fact]
        public void ShouldNotMatterForFacets()
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Admin = true
                    });

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var x = session.Advanced.DocumentQuery<User, UserIndex>()
                        .WhereLucene("Name", "Oren")
                        .WhereLucene("Name", "Eini")
                        .ToFacets(new Facet[]
                        {
                            new Facet<User>
                            {
                                Name = user => user.Admin
                            }
                        });

                    Assert.Empty(x.Results["Admin"].Values);
                }

                using (var session = store.OpenSession())
                {
                    var x = session.Advanced.DocumentQuery<User, UserIndex>()
                        .WhereLucene("Name", "Oren")
                        .WhereLucene("Name", "Eini")
                        .ToFacetsLazy(new Facet[]
                        {
                            new Facet<User>
                            {
                                Name = user => user.Admin
                            }
                        }).Value;

                    Assert.Empty(x.Results["Admin"].Values);
                }
            }
        }
    }
}
