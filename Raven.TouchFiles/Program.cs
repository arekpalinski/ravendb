using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NDesk.Options;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem;

namespace Raven.TouchFiles
{
    class Program
    {
        public static string LogsFilePath = $"logs-{DateTime.Now:g}".Replace("/", "_").Replace(" ", "_").Replace(":", "_");

        private OptionSet options;

        static void Main(string[] args)
        {
            var url = args[0];
            if (url == null)
            {
                Console.WriteLine("First parameter must be server URL");
                return;
            }

            Log("Server url: " + url);

            var unitializedStore = new FilesStore()
            {
                Url = url
            };
            {
                var options = new OptionSet();

                string fileSystem = null;

                NetworkCredential credential = null;
                int batchSize = 1024;
                var startEtag = Etag.Empty;

                options.OnWarning += Console.WriteLine;
                options.Add("u|user|username:", OptionCategory.SmugglerFileSystem, "The username to use when the filesystem requires the client to authenticate.", value =>
                {
                    if (credential == null)
                        credential = new NetworkCredential();

                    credential.UserName = value;
                });
                options.Add("p|pass|password:", OptionCategory.SmugglerFileSystem, "The password to use when the filesystem requires the client to authenticate.", value =>
                {
                    if (credential == null)
                        credential = new NetworkCredential();

                    credential.Password = value;
                });

                options.Add("domain:", OptionCategory.SmugglerFileSystem, "The domain to use when the filesystem requires the client to authenticate.", value =>
                {

                    if (credential == null)
                        credential = new NetworkCredential();

                    credential.Domain = value;
                });
                options.Add("key|api-key|apikey:", OptionCategory.SmugglerFileSystem, "The API-key to use, when using OAuth.", value => { unitializedStore.ApiKey = value; });

                options.Add("f|filesystem:", OptionCategory.SmugglerFileSystem, "The filesystem to operate on. If no specified, the operations will be on the default filesystem.", value => { unitializedStore.DefaultFileSystem = value; });

                options.Add("batch-size:", OptionCategory.SmugglerFileSystem, "The batch size for requests", s => batchSize = int.Parse(s));

                options.Add("start-etag:", OptionCategory.SmugglerFileSystem, "The start etag", s => startEtag = Etag.Parse(s));

                options.Add("help", OptionCategory.SmugglerFileSystem, "help", s => options.WriteOptionDescriptions(Console.Out));

                options.Parse(args);

                
                //////////////

                if (credential != null)
                    unitializedStore.Credentials = credential;

                using (var store = unitializedStore.Initialize())
                {

                    var stats = store.AsyncFilesCommands.GetStatisticsAsync().Result;

                    long total = 0;

                    while (EtagUtil.IsGreaterThan(stats.LastFileEtag, startEtag))
                    {
                        var result = store.AsyncFilesCommands.TouchFilesAsync(startEtag, batchSize).Result;

                        total += result.NumberOfProcessedFiles;

                        Log($"Processed {result.NumberOfProcessedFiles} (filtered: {result.NumberOfFilteredFiles}). Total: {total}. Last processed etag: {result.LastProcessedFileEtag}.");

                        startEtag = result.LastProcessedFileEtag;

                        //var waitPendingCount = 1;

                        //if (store.AsyncFilesCommands.Synchronization.GetPendingAsync(0, 10).Result.TotalCount > 0)
                        //{
                        //    Log("Waiting for clearing pending synchronization queue: ");
                        //    do
                        //    {
                        //        Thread.Sleep(waitPendingCount * 1000);
                        //        waitPendingCount++;

                        //        Log(".", noNewLine: true);

                        //    } while (store.AsyncFilesCommands.Synchronization.GetPendingAsync(0, 10).Result.TotalCount > 2);
                        //}

                        //var waitActiveCount = 1;

                        //if (store.AsyncFilesCommands.Synchronization.GetActiveAsync(0, 10).Result.TotalCount > 0)
                        //{
                        //    Log("Waiting for clearing active synchronization queue: ");
                        //    do
                        //    {
                        //        Thread.Sleep(waitActiveCount * 1000);
                        //        waitPendingCount++;

                        //        Log(".", noNewLine: true);

                        //    } while (store.AsyncFilesCommands.Synchronization.GetActiveAsync(0, 10).Result.TotalCount > 2);
                        //}

                        Thread.Sleep(100);
                    }
                }
            }
        }

        public static void Log(string text, bool noNewLine = false)
        {
            text = $"{DateTime.Now}: {text}";

            if (noNewLine == false)
                Console.WriteLine(text);
            else
                Console.Write(text);

            File.AppendAllText(LogsFilePath, text + Environment.NewLine);
        }
    }
}
