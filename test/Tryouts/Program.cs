using System;
using SlowTests.Bugs;
using SlowTests.Issues;
using FastTests.Voron.Storage;
using SlowTests.Cluster;
using Raven.Server.Documents.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using System.Collections.Generic;

namespace Tryouts
{
    public class Customer
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class SupportCall
    {
        public string Id { get; set; }
        public string CustomerId { get; set; }
        public DateTime Started { get; set; }
        public DateTime? Ended { get; set; }
        public string Issue { get; set; }
        public int Votes { get; set; }
        public List<string> Comments { get; set; }
        public bool Survey { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            RunTest();
            //using (var store = new DocumentStore
            //{
            //    Urls = new string[] { "http://127.0.0.1:8080" },
            //    Database = "test"
            //}.Initialize())
            //{
            //    var options = new SubscriptionCreationOptions<SupportCall>
            //    {
            //        Criteria = new SubscriptionCriteria<SupportCall>(
            //            call =>
            //call.Comments.Count > 25 &&
            //call.Votes > 10 &&
            //!call.Survey 
            //            )
            //    };
            //    store.Subscriptions.Create(options);


            //    //var sub = store.Subscriptions.Open(new Raven.Client.Documents.Subscriptions.SubscriptionConnectionOptions("AllCustomers"));
            //    //sub.Run(batch =>
            //    //{
            //    //    foreach (var item in batch.Items)
            //    //    {
            //    //        Console.WriteLine(item.Id);
            //    //    }
            //    //}).Wait();
            //}
        }

        private static void RunTest()
        {
            for (int i = 0; i < 100; i++)
            {
                Console.Clear();
                Console.WriteLine(i);
                using (var test = new FastTests.Client.Query())
                {
                    try
                    {
                        test.Query_By_Index();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.Beep();
                        return;
                    }
                }
            }
        }
    }
}
