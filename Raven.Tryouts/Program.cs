using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Smuggler;
using Voron;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;


namespace Raven.Tryouts
{

   
    public class Program
    {
        public static void Main(string[] args)
        {

            var options = StorageEnvironmentOptions.ForPath(@"C:\workspace\support\ravendb\sam lee\storage");

            unsafe
            {
                using (var recoveryPager = options.CreateScratchPager(StorageEnvironmentOptions.JournalRecoveryName(17075)))
                using (var pager = options.OpenJournalPager(17075))
                {
                    //RecoverCurrentJournalSize(pager);

                    var transactionHeader = new TransactionHeader()
                    {
                        TransactionId = 95284222
                    };
                    var journalReader = new JournalReader(pager, recoveryPager, -1, &transactionHeader);
                    journalReader.RecoverAndValidate(options);

                    var max = journalReader.TransactionPageTranslation.Max(x => x.Key);

                    var a = journalReader.TransactionPageTranslation.Keys.OrderBy(x => x).ToList();
                }

            }

            using (var env = new StorageEnvironment(options))
            {
                
            }


            //private void RecoverCurrentJournalSize(IVirtualPager pager)
            //{
            //    var journalSize = Utils.NearestPowerOfTwo(pager.NumberOfAllocatedPages * AbstractPager.PageSize);
            //    if (journalSize >= options.MaxLogFileSize) // can't set for more than the max log file size
            //        return;

            //    // this set the size of the _next_ journal file size
            //    _currentJournalFileSize = Math.Min(journalSize, _env.Options.MaxLogFileSize);
            //}


        }
    }
}
