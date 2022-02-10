using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using StressTests.Issues;
using Tests.Infrastructure;
using Random = System.Random;

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
            using (var store = new DocumentStore() { Urls = new[] { "http://localhost:8080" }, Database = "test-02-02" }.Initialize())
            {
                while (true)
                {
                    store.Operations.Send(new PatchByQueryOperation(@"from PortalDepartments where id() = 'PortalDepartments/ZUIDWESTER' update
	{
		for (var index = 0; index < this.Departments.length; ++index)
		{
		var o = this.Departments[index];
		o.Name = o.Name + '_aa_'

	}   
	}
	")).WaitForCompletion();
                }

                var counter = 0;

                var ids = new List<string>();

                using (var session = store.OpenSession())
                {
                    List<dynamic> docs = session.Advanced.RawQuery<dynamic>("from PortalDepartments select id() as Id").ToList();

                    foreach (var doc in docs)
                    {
                        ids.Add(doc.Id.ToString());
                    }


 //                   while (true)
 //                   {
 //                       foreach (string id in ids)
 //                       {
 //                           store.Operations.Send(new PatchByQueryOperation(@"from PortalDepartments where id() = '" + id + @"' update
	//{
	//	for (var index = 0; index < this.Departments.length; ++index)
	//	{
	//	var o = this.Departments[index];
	//	o.Name = o.Name + '_aa_'

	//}   
	//}")).WaitForCompletion();
 //                       }
 //                   }
                }

                for (int i = 0; i < 5; i++)
                {
                    var rand = new Random(counter++);

                    store.Operations.Send(new PatchByQueryOperation(@"from DepartmentMembers update
		{
			
				this.Name = '" + LoremIpsum(1, 4, 1, 1, 1, rand) + @"'  
		}")).WaitForCompletion();

                    store.Operations.Send(new PatchByQueryOperation(@"from DepartmentParents update
		{
			
				this.Name = '" + LoremIpsum(1, 4, 1, 1, 1, rand) + @"'  
		}")).WaitForCompletion();

                    Console.WriteLine(counter);
                   // Console.ReadKey();

                }
            }
        }

        static string LoremIpsum(int minWords, int maxWords,
            int minSentences, int maxSentences,
            int numParagraphs, Random rand)
        {

            var words = new[]{"lorem", "ipsum", "dolor", "sit", "amet", "consectetuer",
                "adipiscing", "elit", "sed", "diam", "nonummy", "nibh", "euismod",
                "tincidunt", "ut", "laoreet", "dolore", "magna", "aliquam", "erat"};

            int numSentences = rand.Next(maxSentences - minSentences)
                               + minSentences + 1;
            int numWords = rand.Next(maxWords - minWords) + minWords + 1;

            StringBuilder result = new StringBuilder();

            for (int p = 0; p < numParagraphs; p++)
            {
                result.Append("<p>");
                for (int s = 0; s < numSentences; s++)
                {
                    for (int w = 0; w < numWords; w++)
                    {
                        if (w > 0)
                        { result.Append(" "); }
                        result.Append(words[rand.Next(words.Length)]);
                    }
                    result.Append(". ");
                }
                result.Append("</p>");
            }

            return result.ToString();
        }
    }
}
