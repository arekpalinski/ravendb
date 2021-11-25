using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
        }

        public static unsafe void Main(string[] args)
        {
            //C:\workspace\ravendb - installations\RavenDB - 5.2.4 - windows - x64\Server\RavenData\Databases\test

            //   var options = StorageEnvironmentOptions.ForPath(args[0]);

            //options.ManualFlushing = true;
            //options.ManualSyncing = true;

            //using (var env = new StorageEnvironment(options))
            //{
            //    var numberOfPagesToRead = int.Parse(args[1]);


            //    if (numberOfPagesToRead > env.NextPageNumber - 1)
            //        throw new InvalidOperationException("Invalid number of pages to read");

            //    const int size = 8192;

            //    // var src = new byte[size];
            //    var dst = new byte[size];

            //    //new System.Random().NextBytes(src);

            //    var lastPrintMemoryUsage = DateTime.UtcNow;

            //    TimeSpan interval = TimeSpan.FromSeconds(5);

            //    while (true)
            //    {
            //        using (var readTx = env.ReadTransaction())
            //        {
            //            var buffers = new List<(IntPtr Ptr, long Size)>();

            //            NativeMemory.ThreadStats thread = null;

            //            for (int i = 0; i < numberOfPagesToRead; i++)
            //            {
            //                //byte* pSrc = env.Options.DataPager.AcquirePagePointer(readTx.LowLevelTransaction, i);


            //                byte* b = EncryptionBuffersPool.Instance.Get(1, out var bufferSize, out thread);

            //                Sparrow.Memory.Set(b, (byte)i, bufferSize);

            //                buffers.Add((new IntPtr(b), bufferSize));

            //                //fixed (byte* pSrc = src)
            //                //fixed (byte* pDst = dst)
            //                //{
            //                //    Sparrow.Memory.Copy(pDst, pSrc, size);
            //                //}

            //                //if (dst[size - 1] != src[size - 1])
            //                //    throw new InvalidOperationException("Error");

            //                if (DateTime.UtcNow - lastPrintMemoryUsage > interval)
            //                {
            //                    var memoryInfo = MemoryInformation.GetMemoryInfo();

            //                    Console.WriteLine(memoryInfo.AvailableMemoryForProcessing);

            //                    lastPrintMemoryUsage = DateTime.UtcNow;

            //                    //new System.Random().NextBytes(src);
            //                }
            //            }

            //            foreach ((IntPtr Ptr, long Size) buffer in buffers)
            //            {
            //                EncryptionBuffersPool.Instance.Return((byte*)buffer.Ptr, buffer.Size, thread, EncryptionBuffersPool.Instance.Generation);
            //            }
            //        }




            //    }
            //}


            using (var store = new DocumentStore()
            {
                Urls = new[] { "https://a.17308-dev.arek-t3st.cloudtest.ravendb.org/" },
                Database = "test3",
                Certificate = new X509Certificate2(@"C:\Users\arek\Desktop\ravendb.cloud.test.master.2021-08-19.pfx", "track-captain-BIRDS-shirt66")

            }.Initialize())
            {
                while (true)
                {
                    using (var session = store.OpenSession())
                    {
                        List<dynamic> objects = session.Advanced.RawQuery<dynamic>("from index UnitServiceSearchIndex where OutletId > 900").NoCaching().ToList();

                        Console.WriteLine("Got " + objects.Count + " results");
                    }
                }
            }
        }
    }
}
