using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Graph;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using StressTests.Issues;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 10_000; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    {
                        using (var test = new GraphPermissionTests(testOutputHelper))
                        {
                            test.MultiplePathsWillAllow("users/2944", "Sunny");
                        }
                    }

                    Console.WriteLine("a");

                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    {
                        using (var test = new GraphPermissionTests(testOutputHelper))
                        {
                            test.MultiplePathsWillAllow("users/84", "Max");
                        }
                    }

                    Console.WriteLine("b");

                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    {
                        using (var test = new GraphPermissionTests(testOutputHelper))
                        {
                            test.MultiplePathsWillAllow("users/12", "Arava");
                        }
                    }

                    Console.WriteLine("c");

                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    {
                        using (var test = new GraphPermissionTests(testOutputHelper))
                        {
                            test.MultiplePathsWillAllow("users/23", "Pheobe");
                        }
                    }

                    Console.WriteLine("d");

                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }

        }
    }
}
