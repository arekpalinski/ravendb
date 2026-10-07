using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Sparrow.Server;
using Voron;
using Voron.Impl.Journal;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// b-tail-shapes: the deterministic complement to b-recycle-kill. A recycled journal still holds
    /// the bytes of whatever was written into it in a previous life, so after reuse the valid records
    /// are followed by an old, perfectly well-formed record. Recovery must stop exactly at the end of
    /// this incarnation's records and never accept one of the old ones.
    ///
    /// Rather than hoping a random run lands on an interesting boundary, this sweeps the shape of the
    /// tail: how many transactions are written into the reused journal (including none at all, so
    /// only the header record is this incarnation's), how big they are relative to the records that
    /// were there before, and whether the file is encrypted (the leftover bytes are ciphertext, so
    /// the garbage distribution is completely different).
    ///
    /// The keys written before recycling are deleted (and the delete synced) before the journals are
    /// reused, so the records still sitting in the recycled file all ADD keys that must no longer
    /// exist. Recovery walking one record too far therefore resurrects them, which the oracle sees.
    ///
    /// Half the shapes end phase B with a torn journal write (SimulatePartialJournalWriteFailure), so
    /// the tail reads: valid records, a half-written record, then the perfectly well-formed records of
    /// the previous incarnation. That is the shape the incarnation marker exists for.
    ///
    /// Oracle: after reopening, the tree holds no resurrected key, holds every key whose commit
    /// returned, and (torn shapes) at most the one transaction that was being torn is missing. A
    /// second reopen must agree with the first.
    /// </summary>
    public static class BTailShapes
    {
        private static readonly int[] NewTxCounts = [0, 1, 2, 3, 5, 9, 17];
        private static readonly int[] OldSizes = [512, 4096, 20000];
        private static readonly int[] NewSizes = [64, 3000, 9000, 40000];

        public static int Run(StressContext ctx)
        {
            var shapes = (from newTxs in NewTxCounts
                          from oldSize in OldSizes
                          from newSize in NewSizes
                          from encrypted in new[] { false, true }
                          from tear in new[] { false, true }
                          where tear == false || newTxs > 0
                          select (newTxs, oldSize, newSize, encrypted, tear)).ToArray();

            var shape = 0;
            var recycledSeen = 0;
            foreach (var (newTxs, oldSize, newSize, encrypted, tear) in shapes)
            {
                if (ctx.TimeLeft == false)
                    break;

                ctx.Iteration = shape;
                ctx.Iterations++;
                shape++;

                var dir = ctx.IterationDir(shape);
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                Directory.CreateDirectory(dir);

                var label = $"newTxs={newTxs} oldSize={oldSize} newSize={newSize} encrypted={encrypted} tear={tear}";
                try
                {
                    if (RunShape(ctx, dir, label, newTxs, oldSize, newSize, encrypted, tear))
                        recycledSeen++;
                }
                catch (Exception e)
                {
                    ctx.Fail($"shape {label}: threw {e}");
                }

                if (ctx.Failures.Count > 0)
                    break;

                Directory.Delete(dir, recursive: true);
            }

            if (ctx.Failures.Count == 0)
            {
                if (recycledSeen == 0)
                    ctx.Fail("no shape ever wrote into a recycled journal, so nothing about recycled tails was tested");
                else
                {
                    Console.WriteLine($"  clean: {shape} tail shapes, {recycledSeen} of them writing into a recycled journal, {RecoveryErrors} raised a recovery error that the handler absorbed");
                    foreach (var m in Messages)
                        Console.WriteLine($"    {m}");
                }
            }

            return ctx.Finish();
        }

        // returns true when the second phase actually landed in a recycled journal
        private static bool RunShape(StressContext ctx, string dir, string label, int newTxs, int oldSize, int newSize, bool encrypted, bool tear)
        {
            var committed = new Dictionary<string, byte>();
            var buried = new List<string>();
            bool reused;
            var tornKey = (string)null;

            using (var env = new StorageEnvironment(CreateOptions(dir, encrypted)))
            {
                // phase A: roll through several journals and sync them, so they are released into the
                // recycle pool with their records still on disk
                var doomed = new Dictionary<string, byte>();
                for (var round = 0; round < 6; round++)
                {
                    Write(env, doomed, $"old/{round}", 24, oldSize);
                    FlushAndSync(env);
                }

                // delete them and sync the delete, so every record left in the recycled files adds a
                // key that must not exist any more - recovery reading one record too far resurrects it
                foreach (var key in doomed.Keys)
                {
                    using var tx = env.WriteTransaction();
                    tx.CreateTree("items").Delete(key);
                    tx.Commit();
                }

                buried.AddRange(doomed.Keys);
                FlushAndSync(env);

                var pool = Directory.GetFiles(JournalPath(env), $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*");
                reused = pool.Length > 0;

                // phase B: write into what should now be a recycled file, leaving the previous
                // incarnation's records in the tail. No flush, no sync - recovery has to replay this.
                if (newTxs > 0)
                {
                    Write(env, committed, "b", tear ? newTxs - 1 : newTxs, newSize);

                    if (tear)
                    {
                        tornKey = $"b/{newTxs - 1:D5}";
                        env.Options.ForTestingPurposesOnly().SimulatePartialJournalWriteFailure = total =>
                            new StorageEnvironmentOptions.TestingStuff.PartialJournalWriteFailure { NumberOf4KbsToWrite = Math.Max(1, total / 2) };

                        try
                        {
                            var buffer = new byte[newSize];
                            Array.Fill(buffer, (byte)(tornKey.GetHashCode() & 0xFF));
                            using var tx = env.WriteTransaction();
                            tx.CreateTree("items").Add(tornKey, buffer);
                            tx.Commit();
                        }
                        catch
                        {
                            // expected: the environment turns catastrophic on the torn write
                        }
                    }
                }
            }

            CurrentLabel = label;
            CurrentWhen = "first reopen";
            var first = ReadAll(dir, encrypted, out var firstError);
            if (firstError != null)
            {
                ctx.Fail($"shape {label}: reopening after the recycled-tail write threw {firstError}");
                return reused;
            }

            if (Compare(ctx, label, "first reopen", committed, buried, tornKey, first) == false)
                return reused;

            CurrentWhen = "second reopen";
            var second = ReadAll(dir, encrypted, out var secondError);
            if (secondError != null)
            {
                ctx.Fail($"shape {label}: the second reopen threw {secondError} after the first one succeeded");
                return reused;
            }

            Compare(ctx, label, "second reopen", committed, buried, tornKey, second);
            return reused;
        }

        private static bool Compare(StressContext ctx, string label, string when, Dictionary<string, byte> expected,
            List<string> buried, string tornKey, Dictionary<string, byte> actual)
        {
            var resurrected = buried.Where(actual.ContainsKey).Take(5).ToArray();
            if (resurrected.Length > 0)
            {
                ctx.Fail($"shape {label}: {when} resurrected keys that were deleted before the journal was recycled, e.g. [{string.Join(", ", resurrected)}] - recovery walked into the previous incarnation's records");
                return false;
            }

            var missing = expected.Keys.Except(actual.Keys).ToArray();
            if (missing.Length > 0)
            {
                ctx.Fail($"shape {label}: {when} lost {missing.Length} keys whose commit returned, e.g. [{string.Join(", ", missing.Take(5))}]");
                return false;
            }

            var extra = actual.Keys.Except(expected.Keys).ToArray();
            // the torn transaction may legitimately be there or not - a reported-failed write can still
            // have reached the disk - but nothing else may be
            var unexplained = extra.Where(k => k != tornKey).Take(5).ToArray();
            if (unexplained.Length > 0)
            {
                ctx.Fail($"shape {label}: {when} recovered keys that were never committed in this incarnation, e.g. [{string.Join(", ", unexplained)}]");
                return false;
            }

            foreach (var (key, stamp) in expected)
            {
                if (actual[key] == stamp)
                    continue;

                ctx.Fail($"shape {label}: {when} key {key} has stamp {actual[key]}, expected {stamp}");
                return false;
            }

            return true;
        }

        private static void Write(StorageEnvironment env, Dictionary<string, byte> committed, string prefix, int count, int size)
        {
            var buffer = new byte[size];
            for (var i = 0; i < count; i++)
            {
                var key = $"{prefix}/{i:D5}";
                var stamp = (byte)(key.GetHashCode() & 0xFF);
                Array.Fill(buffer, stamp);

                using var tx = env.WriteTransaction();
                tx.CreateTree("items").Add(key, buffer);
                tx.Commit();

                committed[key] = stamp;
            }
        }

        internal static int RecoveryErrors;
        internal static readonly List<string> Messages = [];
        internal static string CurrentLabel = "";
        internal static string CurrentWhen = "";

        private static Dictionary<string, byte> ReadAll(string dir, bool encrypted, out Exception error)
        {
            error = null;
            var result = new Dictionary<string, byte>();
            try
            {
                var options = CreateOptions(dir, encrypted);
                // the server subscribes to this, so recovery logs and stops at the bad record instead
                // of throwing; without a listener InvokeRecoveryError throws and we would only ever be
                // measuring the absence of a handler
                options.OnRecoveryError += (_, e) =>
                {
                    Interlocked.Increment(ref RecoveryErrors);
                    if (Messages.Count < 12)
                        Messages.Add($"{CurrentLabel} [{CurrentWhen}]: {e.Message}");
                };

                using var env = new StorageEnvironment(options);
                using var tx = env.ReadTransaction();
                var tree = tx.ReadTree("items");
                if (tree == null)
                    return result;

                using var it = tree.Iterate(prefetch: false);
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return result;

                do
                {
                    var reader = it.CreateReaderForCurrent();
                    var b = new byte[1];
                    reader.Read(b, 0, 1);
                    result[it.CurrentKey.ToString()] = b[0];
                } while (it.MoveNext());
            }
            catch (Exception e)
            {
                error = e;
            }

            return result;
        }

        private static void FlushAndSync(StorageEnvironment env)
        {
            env.FlushLogToDataFile();
            using var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator);
            operation.SyncDataFile();
        }

        private static string JournalPath(StorageEnvironment env) =>
            ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)env.Options).JournalPath.FullPath;

        // the master key has to be stable across reopens of the same directory
        private static readonly byte[] MasterKey = Enumerable.Range(0, 32).Select(i => (byte)(i * 7 + 3)).ToArray();

        private static StorageEnvironmentOptions CreateOptions(string dir, bool encrypted)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(dir);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 128 * 1024;
            options.MaxNumberOfRecyclableJournals = 32;
            options.EnableJournalPoolPrewarming = false; // the pool must only hold files we released

            if (encrypted)
                options.Encryption.MasterKey = MasterKey.ToArray();

            return options;
        }
    }
}
