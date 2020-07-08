using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Voron;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public class Community
        {
            public string CommunityId { get; set; }
            public int Count { get; set; }
        }
        
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore()
            {
                Urls = new []{ "http://localhost:8081/" },
                Database = "uat-release-2"
            }.Initialize())
            {
                while (true)
                {


                    using (var session = store.OpenSession())
                    {
                        IRawDocumentQuery<Community> communities = session.Advanced.RawQuery<Community>(@"from index 'aa2'");

                        //foreach (Community communityId in communities)
                        //{

                            store.Operations.Send(new PatchByQueryOperation(@"from UserDocuments update
{
	var groupId = 'urcbgk4scj61d3';
	

	for (var i = 0; i < this.MemberOfCommunities.length; i++) 
	{
		var memberShip = this.MemberOfCommunities[i];

		//if (memberShip.CommunityId === communityId)
		//{
		//	if (memberShip.GroupIds.some(id => id === groupId) === false)
		//	{ 
				memberShip.GroupIds.push(groupId);
		//	}
		//} 
	}
}")).WaitForCompletion();

                            Console.WriteLine("A");
                        //}
                    }

                    store.Operations.Send(new PatchByQueryOperation(@"from UserDocuments update
{
	var groupId = 'urcbgk4scj61d3';

	for (var i = 0; i < this.MemberOfCommunities.length; i++) {
		var memberShip = this.MemberOfCommunities[i];
		var groupIndex = memberShip.GroupIds.indexOf(groupId);
		if (groupIndex > -1)
		{
			memberShip.GroupIds.splice(groupIndex, 1);
		}
	}
}")).WaitForCompletion();

                    Console.WriteLine(".");
                    Thread.Sleep(2000);
                }
            }

            //Console.WriteLine(Process.GetCurrentProcess().Id);
            //for (int i = 0; i < 123; i++)
            //{
            //    Console.WriteLine($"Starting to run {i}");
            //    try
            //    {
            //        using (var testOutputHelper = new ConsoleTestOutputHelper())
            //        using (var test = new RavenDB_13940(testOutputHelper))
            //        {
            //            test.CorruptedSingleTransactionPage_WontStopTheRecoveryIfIgnoreErrorsOfSyncedTransactionIsSet();
            //        }
            //    }
            //    catch (Exception e)
            //    {
            //        Console.ForegroundColor = ConsoleColor.Red;
            //        Console.WriteLine(e);
            //        Console.ForegroundColor = ConsoleColor.White;
            //       // Console.ReadLine();
            //    }
            //}
        }
    }
}
