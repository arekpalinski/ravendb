using System;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_27846 : StorageTest
    {
        public RavenDB_27846(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CommitFailsWhenJournalEntryHeaderIsNotOurs()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("key", "value");

                using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => tx.LowLevelTransaction.TransactionHeader.Hash ^= 1))
                {
                    var e = Assert.Throws<InvalidOperationException>(() => tx.Commit());
                    Assert.Contains("belongs to another transaction", e.Message);
                }
            }

            // the flusher thread may wrap the catastrophic failure in VoronUnrecoverableErrorException first
            var catastrophic = Assert.ThrowsAny<Exception>(() => Env.WriteTransaction());
            Assert.Contains("belongs to another transaction", catastrophic.ToString());
        }
    }
}
