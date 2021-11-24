using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Sparrow.LowMemory;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
        }

        public static unsafe void Main(string[] args)
        {
            const int size = 8192;

            var src = new byte[size];
            var dst = new byte[size];

            new System.Random().NextBytes(src);

            var lastPrintMemoryUsage = DateTime.UtcNow;

            TimeSpan interval = TimeSpan.FromSeconds(5);

            while (true)
            {
                fixed (byte* pSrc = src)
                fixed (byte* pDst = dst)
                {
                    Sparrow.Memory.Copy(pDst, pSrc, size);
                }

                if (dst[size - 1] != src[size - 1])
                    throw new InvalidOperationException("Error");

                if (DateTime.UtcNow - lastPrintMemoryUsage > interval)
                {
                    var memoryInfo = MemoryInformation.GetMemoryInfo();

                    Console.WriteLine(memoryInfo.AvailableMemoryForProcessing);

                    lastPrintMemoryUsage = DateTime.UtcNow;

                    new System.Random().NextBytes(src);
                }
            }


            //using (var store = new DocumentStore()
            //{
            //    Urls = new[] { "https://a.17308-dev.arek-t3st.cloudtest.ravendb.org/" },
            //    Database = "test3",
            //    Certificate = new X509Certificate2(@"C:\Users\arek\Desktop\ravendb.cloud.test.master.2021-08-19.pfx", "track-captain-BIRDS-shirt66")

            //}.Initialize())
            //{
            //    while (true)
            //    {
            //        using (var session = store.OpenSession())
            //        {
            //            List<dynamic> objects = session.Advanced.RawQuery<dynamic>("from index UnitServiceSearchIndex where OutletId > 900").NoCaching().ToList();

            //            Console.WriteLine("Got " + objects.Count + " results");
            //        }
            //    }
            //}
        }
    }
}
