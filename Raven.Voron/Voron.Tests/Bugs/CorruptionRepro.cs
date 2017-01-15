// -----------------------------------------------------------------------
//  <copyright file="CorruptionRepro.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
    public class CorruptionRepro : StorageTest
    {
        [Fact]
        public void Foo()
        {
            var numberOfEntries = 0;

            using (var reader = new StreamReader("Bugs/Data/VoronTestBefore.CSV"))
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = Env.CreateTree(tx, "key_by_etag");

                    do
                    {
                        var entry = reader.ReadLine().Split(',');

                        var etag = entry[1];
                        var key = entry[0];

                        tree.Add(etag, key);

                        numberOfEntries++;

                    } while (reader.EndOfStream == false);

                    Assert.Equal(numberOfEntries, tree.State.EntriesCount);

                    tx.Commit();
                }
            }

            var alreadyAddedEtags = new HashSet<string>();

            using (var reader1 = new StreamReader("Bugs/Data/VoronTestBefore.CSV"))
            using (var reader2 = new StreamReader("Bugs/Data/VoronTestAfter.CSV"))
            {
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = Env.CreateTree(tx, "key_by_etag");

                    do
                    {
                        var existingEntry = reader1.ReadLine().Split(',');
                        var newEntry = reader2.ReadLine().Split(',');

                        var oldEtag = existingEntry[1];

                        if (alreadyAddedEtags.Contains(oldEtag) == false)
                            tree.Delete(oldEtag);

                        var etag = newEntry[1];
                        var key = newEntry[0];
                        tree.Add(etag, key);

                        alreadyAddedEtags.Add(etag);
                    } while (reader1.EndOfStream == false && reader2.EndOfStream == false);

                    tx.Commit();
                }
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("key_by_etag");

                using (var it = tree.Iterate())
                {
                    Assert.True(it.Seek(Slice.BeforeAllKeys));

                    var count = 0;
                    do
                    {
                        count++;
                    } while (it.MoveNext());

                    Assert.Equal(numberOfEntries, count);
                }
            }
        }
    }
}