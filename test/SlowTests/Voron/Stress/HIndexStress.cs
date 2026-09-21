using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// h-index-stress: Table fixed-size indexes resolved by schema position (CachePosition flat array)
    /// with index DEFINITION INSTANCES SHARED between a plain and a compressed schema, the way the
    /// documents schemas share theirs. 12 tables alternate between the two schemas. Each transaction
    /// sets (insert/overwrite with a new etag, new secondary value and a new row size) and deletes rows
    /// in several tables. After every batch, per table: row count, per-table etag index enumeration
    /// (ordered etags + the key each maps to), secondary index multiset, and the GLOBAL etag index
    /// (queried through a random table) are compared with a model; ReadByKey samples check the stored
    /// etag. The environment is restarted per iteration and re-verified with freshly created definition
    /// instances (positions re-assigned). Debug build keeps Table's position/name asserts live.
    /// </summary>
    public static unsafe class HIndexStress
    {
        private const int TableCount = 12;
        private const int BatchesPerIteration = 30;

        private sealed record Row(long Etag, long Secondary);

        private sealed class Schemas : IDisposable
        {
            public readonly ByteStringContext Allocator = new(SharedMultipleUseFlag.None); // index names must outlive any transaction
            public TableSchema Plain;
            public TableSchema Compressed;
            public TableSchema.FixedSizeKeyIndexDef GlobalEtags;
            public TableSchema.FixedSizeKeyIndexDef TableEtags;
            public TableSchema.FixedSizeKeyIndexDef Secondary;

            public TableSchema For(int table) => table % 2 == 0 ? Plain : Compressed;

            public void Dispose() => Allocator.Dispose();
        }

        public static int Run(StressContext ctx)
        {
            for (var iteration = 0; ctx.TimeLeft; iteration++)
            {
                ctx.Iteration = iteration;
                ctx.Iterations++;
                var dir = ctx.IterationDir(iteration);
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                Directory.CreateDirectory(dir);

                try
                {
                    RunIteration(ctx, dir, unchecked(ctx.Seed * 6007 + iteration));
                }
                catch (Exception e)
                {
                    ctx.Fail($"iteration {iteration} threw: {e}");
                }

                if (ctx.Failures.Count > 0)
                    break;

                try { Directory.Delete(dir, recursive: true); } catch { }
            }

            return ctx.Finish();
        }

        private static Schemas CreateSchemas()
        {
            // one definition instance per logical index, shared by both schema variants, defined in the same order
            var s = new Schemas();
            Slice.From(s.Allocator, "GlobalEtags", out var globalEtagsName);
            Slice.From(s.Allocator, "TableEtags", out var tableEtagsName);
            Slice.From(s.Allocator, "Secondary", out var secondaryName);

            s.GlobalEtags = new TableSchema.FixedSizeKeyIndexDef { Name = globalEtagsName, IsGlobal = true, StartIndex = 1 };
            s.TableEtags = new TableSchema.FixedSizeKeyIndexDef { Name = tableEtagsName, IsGlobal = false, StartIndex = 1 };
            s.Secondary = new TableSchema.FixedSizeKeyIndexDef { Name = secondaryName, IsGlobal = false, StartIndex = 2 };

            TableSchema Build(bool compress) => new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineFixedSizeIndex(s.GlobalEtags)
                .DefineFixedSizeIndex(s.TableEtags)
                .DefineFixedSizeIndex(s.Secondary)
                .CompressValues(s.TableEtags, compress);

            s.Plain = Build(false);
            s.Compressed = Build(true);
            return s;
        }

        private static void RunIteration(StressContext ctx, string dir, int seed)
        {
            var rng = new Random(seed);
            var models = new Dictionary<string, Row>[TableCount];
            for (var i = 0; i < TableCount; i++)
                models[i] = new Dictionary<string, Row>();
            long etag = 0;

            using (var env = new StorageEnvironment(CreateOptions(dir)))
            using (var schemas = CreateSchemas())
            {
                using (var tx = env.WriteTransaction())
                {
                    for (var i = 0; i < TableCount; i++)
                        schemas.For(i).Create(tx, TableName(i), 16);
                    tx.Commit();
                }

                for (var batch = 0; batch < BatchesPerIteration; batch++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tables = 1 + rng.Next(3);
                        for (var t = 0; t < tables; t++)
                        {
                            var idx = rng.Next(TableCount);
                            var table = tx.OpenTable(schemas.For(idx), TableName(idx));
                            var model = models[idx];
                            var ops = 20 + rng.Next(180);
                            for (var o = 0; o < ops; o++)
                            {
                                var key = $"k/{rng.Next(3000):D5}";
                                if (rng.Next(100) < 70 || model.Count == 0)
                                {
                                    // fixed-size index keys must be unique per index: embed the etag, scramble the order with a high byte
                                    var row = new Row(++etag, ((long)rng.Next(256) << 40) | etag);
                                    Set(tx, table, key, row, 100 + rng.Next(1900));
                                    model[key] = row;
                                }
                                else
                                {
                                    using (Slice.From(tx.Allocator, key, out var keySlice))
                                    {
                                        var existed = table.DeleteByKey(keySlice);
                                        if (existed != model.Remove(key))
                                        {
                                            ctx.Fail($"iteration {ctx.Iteration} batch {batch}: DeleteByKey({key}) on {TableName(idx)} returned {existed}, model said {!existed}");
                                            return;
                                        }
                                    }
                                }
                            }
                        }

                        tx.Commit();
                    }

                    if (batch % 10 == 9)
                        env.FlushLogToDataFile();

                    if (Verify(ctx, env, schemas, models, rng, $"iteration {ctx.Iteration} batch {batch}") == false)
                        return;
                }
            }

            // restart: fresh definition instances, positions re-assigned, tables re-opened against them
            using (var env = new StorageEnvironment(CreateOptions(dir)))
            using (var schemas = CreateSchemas())
            {
                if (Verify(ctx, env, schemas, models, rng, $"iteration {ctx.Iteration} after restart") == false)
                    return;
            }

            Console.WriteLine($"  iter {ctx.Iteration}: {BatchesPerIteration} batches, {models.Sum(m => m.Count)} live rows across {TableCount} tables (last etag {etag}), verified after every batch + restart");
        }

        private static void Set(Transaction tx, Table table, string key, Row row, int payloadSize)
        {
            using (table.Allocate(out TableValueBuilder b))
            using (Slice.From(tx.Allocator, key, out var keySlice))
            {
                b.Add(keySlice);
                b.Add(Bits.SwapBytes(row.Etag));
                b.Add(Bits.SwapBytes(row.Secondary));
                var payload = stackalloc byte[2048];
                new Span<byte>(payload, payloadSize).Fill((byte)(row.Etag % 251));
                b.Add(payload, payloadSize);
                table.Set(b);
            }
        }

        private static bool Verify(StressContext ctx, StorageEnvironment env, Schemas schemas, Dictionary<string, Row>[] models, Random rng, string stage)
        {
            using var tx = env.ReadTransaction();
            var allEtags = new List<long>();

            for (var i = 0; i < TableCount; i++)
            {
                var table = tx.OpenTable(schemas.For(i), TableName(i));
                var model = models[i];
                allEtags.AddRange(model.Values.Select(r => r.Etag));

                if (table.NumberOfEntries != model.Count)
                {
                    ctx.Fail($"{stage}: {TableName(i)} NumberOfEntries {table.NumberOfEntries} != model {model.Count}");
                    return false;
                }

                if (table.GetNumberOfEntriesFor(schemas.TableEtags) != model.Count || table.GetNumberOfEntriesFor(schemas.Secondary) != model.Count)
                {
                    ctx.Fail($"{stage}: {TableName(i)} index counts (etags {table.GetNumberOfEntriesFor(schemas.TableEtags)}, secondary {table.GetNumberOfEntriesFor(schemas.Secondary)}) != model {model.Count}");
                    return false;
                }

                // per-table etag index: ordered etags, each mapping back to the right key
                var expectedByEtag = model.OrderBy(kv => kv.Value.Etag).ToList();
                var pos = 0;
                foreach (var reader in table.SeekForwardFrom(schemas.TableEtags, 0, 0))
                {
                    if (pos >= expectedByEtag.Count)
                    {
                        ctx.Fail($"{stage}: {TableName(i)} etag index yields more rows than the model ({model.Count})");
                        return false;
                    }

                    var (key, row) = (expectedByEtag[pos].Key, expectedByEtag[pos].Value);
                    var gotEtag = Bits.SwapBytes(*(long*)reader.Read(1, out _));
                    var gotKey = Encoding.ASCII.GetString(reader.Read(0, out var keySize), keySize);
                    if (gotEtag != row.Etag || gotKey != key)
                    {
                        ctx.Fail($"{stage}: {TableName(i)} etag index position {pos}: got ({gotKey}, etag {gotEtag}), expected ({key}, etag {row.Etag})");
                        return false;
                    }

                    pos++;
                }

                if (pos != expectedByEtag.Count)
                {
                    ctx.Fail($"{stage}: {TableName(i)} etag index yields {pos} rows, model has {expectedByEtag.Count}");
                    return false;
                }

                // secondary index: same multiset of values
                var expectedSecondary = model.Values.Select(r => r.Secondary).OrderBy(v => v).ToList();
                var gotSecondary = table.SeekForwardFrom(schemas.Secondary, 0, 0).Select(r => Bits.SwapBytes(*(long*)r.Read(2, out _))).ToList();
                if (gotSecondary.SequenceEqual(expectedSecondary) == false)
                {
                    ctx.Fail($"{stage}: {TableName(i)} secondary index diverged ({gotSecondary.Count} vs {expectedSecondary.Count} entries)");
                    return false;
                }

                // ReadByKey samples
                foreach (var kv in model.Take(20))
                {
                    using (Slice.From(tx.Allocator, kv.Key, out var keySlice))
                    {
                        if (table.ReadByKey(keySlice, out var reader) == false || Bits.SwapBytes(*(long*)reader.Read(1, out _)) != kv.Value.Etag)
                        {
                            ctx.Fail($"{stage}: {TableName(i)} ReadByKey({kv.Key}) missing or wrong etag (expected {kv.Value.Etag})");
                            return false;
                        }
                    }
                }
            }

            // global etag index, queried through a random table, must see every table's rows
            var through = rng.Next(TableCount);
            var globalTable = tx.OpenTable(schemas.For(through), TableName(through));
            var expectedGlobal = allEtags.OrderBy(e => e).ToList();
            var gotGlobal = globalTable.SeekForwardFrom(schemas.GlobalEtags, 0, 0).Select(r => Bits.SwapBytes(*(long*)r.Read(1, out _))).ToList();
            if (gotGlobal.SequenceEqual(expectedGlobal) == false)
            {
                var missing = expectedGlobal.Except(gotGlobal).Take(5);
                var extra = gotGlobal.Except(expectedGlobal).Take(5);
                ctx.Fail($"{stage}: global etag index (via {TableName(through)}) diverged: {gotGlobal.Count} vs {expectedGlobal.Count} (missing {string.Join(",", missing)}; unexpected {string.Join(",", extra)})");
                return false;
            }

            return true;
        }

        private static string TableName(int i) => $"t/{i:D2}";

        private static StorageEnvironmentOptions CreateOptions(string dir)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.EnableJournalPoolPrewarming = false;
            return options;
        }
    }
}
