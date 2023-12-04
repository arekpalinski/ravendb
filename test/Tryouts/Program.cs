using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using RachisTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using SlowTests.Client.Attachments;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            using var store = new DocumentStore
            {
                Urls = new[] { "http://127.0.0.1:8080" },
                Database = "test",
                Conventions = new DocumentConventions
                {
                    //HttpVersion = HttpVersion.Version20
                }
            };
            store.Initialize();

            var cts = new CancellationTokenSource();

            var tasks = new List<Task>();
            for (int j = 0; j < 100; ++j)
            {
                var c = j;
                tasks.Add(Task.Run(async () =>
                {
                    Console.WriteLine($"Created {c}");

                    while (true)
                    {
                        await StreamQueryAsync(store, cts);

                        if (cts.IsCancellationRequested)
                            return;
                    }
                }));
            }

            while (true)
            {
                var q = Console.ReadLine();
                if (string.Equals(q, "q"))
                {
                    cts.Cancel();
                    await Task.WhenAll(tasks);
                    break;
                }
            }

            async Task StreamQueryAsync(DocumentStore store, CancellationTokenSource cts)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncRawQuery<dynamic>("from index 'Orders/Totals'");

                    var stream = await session.Advanced.StreamAsync(query);
                    while (await stream.MoveNextAsync())
                    {


                        if (cts.IsCancellationRequested)
                            return;
                    }
                }
            }
        }
    }
}
