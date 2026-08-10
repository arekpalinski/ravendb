using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace Tryouts.RavenDB_24520;

// Scenario 1 driver: restore a fresh work dir from golden, apply ONE corruption cell, restart, record outcome.
// Every cell is reproducible from the runbook: `cell <name> <op> <ownerFilter> <which> [fileSelector]`.
public static class Scenarios
{
    public static string FindingsFile => Path.Combine(Harness.BaseDir, "findings-scenario1.md");

    public static async Task<int> RunCellAsync(string[] args)
    {
        // args: cell <name> <op> <ownerFilter> <which> [fileSelector]
        if (args.Length < 5)
        {
            Console.WriteLine("""
                cell <name> <op> <ownerFilter> <which> [fileSelector]
                  op:          payload | marker | hash | txid | journalid | zero-block | truncate-tail | truncate-mid | delete | diverge | linkrecord
                  ownerFilter: exact index dir name (e.g. Questions_Tags), '@SharedJournals', '<link-record>', or 'any'
                  which:       first | last | index:N   (selects among matching txs in the chosen file)
                  fileSelector: shared | branch:<name> | inode:first | inode:last (default: shared)
                """);
            return 1;
        }

        var name = args[1];
        var op = args[2].ToLowerInvariant();
        var ownerFilter = args[3];
        var which = args[4];
        var fileSelector = args.Length > 5 ? args[5] : "shared";

        Console.WriteLine($"===== CELL {name}: op={op} owner={ownerFilter} which={which} file={fileSelector} =====");
        Harness.RestoreWorkDirFromGolden();
        if (Directory.Exists(Harness.LogsDir))
            Directory.Delete(Harness.LogsDir, recursive: true); // scope the log scan to this cell only

        var envs = JournalTools.DiscoverEnvironments(Harness.WorkDir);
        var journalPath = SelectFile(Harness.WorkDir, fileSelector);
        Console.WriteLine($"[cell] target file: {Path.GetRelativePath(Harness.WorkDir, journalPath)}");

        var beforeLinks = Harness.GetHardLinkCount(journalPath);
        var applied = ApplyOp(op, journalPath, envs, ownerFilter, which);
        if (applied == false)
        {
            Console.WriteLine("[cell] NO MATCHING TARGET - skipping");
            return 2;
        }

        // report link count after (diverge/delete change topology)
        if (File.Exists(journalPath))
            Console.WriteLine($"[cell] links {beforeLinks} -> {Harness.GetHardLinkCount(journalPath)}");

        var consoleOut = Console.Out;
        var capture = new StringWriter();
        Console.SetOut(new DualWriter(consoleOut, capture));
        bool ok;
        try
        {
            ok = await Harness.VerifyAsync(Harness.WorkDir);
        }
        catch (Exception e)
        {
            ok = false;
            Console.WriteLine($"[cell] verify threw: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            Console.SetOut(consoleOut);
        }

        RecordFinding(name, op, ownerFilter, which, fileSelector, capture.ToString());
        Console.WriteLine($"[cell] {name} recorded (verify ok={ok})");
        return 0;
    }

    private static bool ApplyOp(string op, string journalPath, List<JournalTools.EnvInfo> envs, string ownerFilter, string which)
    {
        // ops that don't need a specific tx
        switch (op)
        {
            case "delete":
                JournalTools.DeleteJournal(journalPath);
                return true;
            case "truncate-tail":
            {
                var len = new FileInfo(journalPath).Length;
                JournalTools.TruncateAt(journalPath, Math.Max(0, len - 4096)); // drop last 4KB block
                return true;
            }
            case "diverge":
                JournalTools.DivergeCopy(journalPath, flipByteAt: null);
                return true;
        }

        var target = SelectTarget(journalPath, envs, ownerFilter, which);
        if (target == null)
            return false;

        Console.WriteLine($"[cell] target tx: {target}");
        return ApplyOpToTarget(op, journalPath, target);
    }

    // Selects the transaction a cell aims at. Shared by the at-rest `cell` driver and the live
    // `corrupt-live` driver, which needs the target in hand *before* corrupting it so it can check
    // the target's sync state (see the sync-state methodology note in the runbooks).
    public static JournalTools.TxEntry SelectTarget(string journalPath, List<JournalTools.EnvInfo> envs, string ownerFilter, string which)
    {
        var txs = JournalTools.Parse(journalPath, envs);
        var matches = txs.Where(t => OwnerMatches(t, ownerFilter)).ToList();
        if (matches.Count == 0)
            return null;

        return which switch
        {
            "first" => matches[0],
            "last" => matches[^1],
            _ when which.StartsWith("index:") => matches[int.Parse(which["index:".Length..])],
            _ => throw new ArgumentException($"bad which: {which}")
        };
    }

    public static bool ApplyOpToTarget(string op, string journalPath, JournalTools.TxEntry target)
    {
        switch (op)
        {
            case "payload":
                JournalTools.FlipPayloadBytes(journalPath, target, offsetInPayload: 0, count: 4);
                break;
            case "marker":
                JournalTools.SmashHeaderMarker(journalPath, target);
                break;
            case "hash":
                JournalTools.CorruptHeaderHash(journalPath, target);
                break;
            case "txid":
                JournalTools.CorruptHeaderTxId(journalPath, target);
                break;
            case "journalid":
                JournalTools.CorruptHeaderJournalId(journalPath, target);
                break;
            case "zero-block":
                JournalTools.ZeroBlock(journalPath, target.ByteOffset / 4096, blocks: 1);
                break;
            case "truncate-mid":
                JournalTools.TruncateAt(journalPath, target.ByteOffset + 2048); // cut through this tx's header
                break;
            case "linkrecord":
                // corrupt the payload of a link-record specifically
                if (target.IsLinkRecord == false)
                {
                    Console.WriteLine("[cell] selected tx is not a link record");
                    return false;
                }
                JournalTools.FlipPayloadBytes(journalPath, target, offsetInPayload: 0, count: 4);
                break;
            default:
                throw new ArgumentException($"unknown op: {op}");
        }
        return true;
    }

    private static bool OwnerMatches(JournalTools.TxEntry tx, string ownerFilter)
    {
        if (ownerFilter == "any")
            return true;
        if (ownerFilter == "<link-record>")
            return tx.IsLinkRecord;
        // exact owner (index dir name) match - avoids "Questions_Tags" also matching "Questions_Tags_ByMonths"
        return string.Equals(tx.Owner, ownerFilter, StringComparison.OrdinalIgnoreCase);
    }

    private static string SelectFile(string dataDir, string selector)
    {
        var shared = Harness.SharedJournalFiles(dataDir).ToList();
        if (selector == "shared" || selector == "inode:last")
            return shared.Last(); // active (highest-numbered) shared journal
        if (selector == "inode:first")
            return shared.First();
        if (selector.StartsWith("branch:"))
        {
            var idxName = selector["branch:".Length..];
            var match = Harness.BranchJournalFiles(dataDir).Where(f => f.Contains(idxName, StringComparison.OrdinalIgnoreCase)).ToList();
            if (match.Count == 0)
                throw new InvalidOperationException($"no branch journal for {idxName}");
            return match.Last();
        }
        throw new ArgumentException($"bad fileSelector: {selector}");
    }

    // ---------------------------------------------------------------- Scenario 1G: corrupt WHILE the server runs
    //
    // Linux-only. Windows opens journals with restrictive share modes, so an external process cannot
    // rewrite them mid-run; Linux has no mandatory locking and Voron takes no advisory lock on journal
    // files (the only flock in the tree guards the secret-key file), so we can corrupt a journal the
    // server is holding open.
    //
    // What the code says to expect: a running environment never READS journal bytes. Flush pushes pages
    // from scratch to the data file (WriteAheadJournal.ApplyPagesToDataFileFromScratch); the only journal
    // readers are recovery (Options.OpenJournalPager, WriteAheadJournal recovery loop) and incremental
    // backup. So passive live corruption should be invisible until restart - and the one live path that
    // re-reads the file without a restart is a branch environment opening, which an index RESET triggers
    // (Index.cs -> IndexStore.RegisterSharedJournals). Both are probed here.
    //
    // Usage: corrupt-live <name> <op> <ownerFilter> <which> [fileSelector] [--probe passive|reset|both] [--observe <sec>]
    public static async Task<int> RunCorruptLiveAsync(string[] args)
    {
        if (args.Length < 5)
        {
            Console.WriteLine("""
                corrupt-live <name> <op> <ownerFilter> <which> [fileSelector] [--probe passive|reset|both] [--observe <sec>]
                  op / ownerFilter / which / fileSelector: same meaning as `cell`
                  --probe   passive (corrupt and just watch) | reload (also disable+enable the database to
                            force in-process recovery with the ORIGINAL JournalIds) | both  (default: both)
                  --observe seconds to watch after each corruption step (default 30)
                """);
            return 1;
        }

        var name = args[1];
        var op = args[2].ToLowerInvariant();
        var ownerFilter = args[3];
        var which = args[4];
        var fileSelector = args.Length > 5 && args[5].StartsWith("--") == false ? args[5] : "shared";
        var probe = ArgValue(args, "--probe") ?? "both";
        var observeSec = int.Parse(ArgValue(args, "--observe") ?? "30");

        Console.WriteLine($"===== CORRUPT-LIVE {name}: op={op} owner={ownerFilter} which={which} file={fileSelector} probe={probe} observe={observeSec}s =====");
        Harness.RestoreWorkDirFromGolden();
        if (Directory.Exists(Harness.LogsDir))
            Directory.Delete(Harness.LogsDir, recursive: true); // scope the log scan to this run only

        var report = new StringBuilder();
        using var server = ServerProcess.Start(Harness.WorkDir);
        using var store = Harness.OpenStore();
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };

        // Settle first: parsing a journal the merger is actively appending to could pick a half-written
        // transaction, which would be an artifact of the harness rather than a real corruption.
        Console.WriteLine("[live] priming + settling indexing so the target tx is fully committed ...");
        await Harness.WriteBurstAsync(store, 2_000, startOffset: 400_000);
        await Harness.WaitForIndexingAsync(store, TimeSpan.FromMinutes(5));

        var envs = JournalTools.DiscoverEnvironments(Harness.WorkDir);
        var journalPath = SelectFile(Harness.WorkDir, fileSelector);
        Console.WriteLine($"[live] target file: {Path.GetRelativePath(Harness.WorkDir, journalPath)} (links={Harness.GetHardLinkCount(journalPath)})");

        var target = SelectTarget(journalPath, envs, ownerFilter, which);
        if (target == null && op is not ("delete" or "truncate-tail" or "diverge"))
        {
            Console.WriteLine("[live] NO MATCHING TARGET - skipping");
            return 2;
        }

        // Sync-state guard. A tx the owner has already synced past is skipped UNVALIDATED by recovery
        // (JournalReader.IsAlreadySyncTransaction), so corrupting one proves nothing. Report it loudly
        // rather than silently producing a meaningless clean result.
        if (target != null)
        {
            Console.WriteLine($"[live] target tx: {target}");
            var synced = DescribeSyncState(journalPath, target, envs);
            Console.WriteLine($"[live] sync state of target: {synced}");
            report.AppendLine($"target: {target}");
            report.AppendLine($"sync state: {synced}");
        }

        var before = await SnapshotAsync(store);
        Console.WriteLine($"[live] before corruption: {before}");
        var baselineErrors = await CountIndexErrorsAsync(store);

        if (op is "delete" or "truncate-tail" or "diverge")
            ApplyFileLevelOp(op, journalPath);
        else
            ApplyOpToTarget(op, journalPath, target);

        // ---- Phase A: passive. Keep writing; does the RUNNING server notice?
        Console.WriteLine($"[live] PHASE A (passive): writing + watching for {observeSec}s ...");
        try
        {
            await Harness.WriteBurstAsync(store, 2_000, startOffset: 500_000);
        }
        catch (Exception e)
        {
            Console.WriteLine($"[live]   write burst threw: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        var phaseA = await ObserveAsync(store, server, observeSec, baselineErrors);
        Console.WriteLine($"[live] PHASE A result: {phaseA}");
        report.AppendLine($"phase A (passive): {phaseA}");

        // ---- Phase B: force a live RECOVERY of the existing environments, in-process.
        //
        // NOT via index RESET: a reset index gets a fresh JournalId, so post-27278 the owner filter skips
        // every pre-existing transaction unvalidated and the corrupted one becomes unreachable. Resetting
        // would therefore be guaranteed not to detect anything AND would destroy what phase C measures.
        //
        // Disable + enable the database instead. That unloads every environment and reloads it with its
        // ORIGINAL JournalId, so recovery genuinely replays and validates the corrupted transaction while
        // the server process stays up - the only live path that does.
        if (probe is "reload" or "both")
        {
            Console.WriteLine("[live] PHASE B: disable + enable the database -> in-process recovery with the ORIGINAL JournalIds");
            try
            {
                await store.Maintenance.Server.SendAsync(new Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation(Harness.DbName, disable: true));
                await Task.Delay(2000);
                await store.Maintenance.Server.SendAsync(new Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation(Harness.DbName, disable: false));
            }
            catch (Exception e)
            {
                Console.WriteLine($"[live]   toggle threw: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            }

            var phaseB = await ObserveAsync(store, server, observeSec, baselineErrors);
            Console.WriteLine($"[live] PHASE B (reload) result: {phaseB}");
            report.AppendLine($"phase B (db disable+enable, original JournalIds): {phaseB}");
            Console.WriteLine($"[live] after reload: {await SnapshotAsync(store)}");
            report.AppendLine($"after reload: {await SnapshotAsync(store)}");
        }

        var liveLogHits = Harness.CountLogMatches("VoronUnrecoverableErrorException", "SetException", "MarkCatastrophicFailure", "InvalidJournalException", "CatastrophicFailure");
        Console.WriteLine($"[live] journal/corruption markers in the log while running: {liveLogHits}");
        report.AppendLine($"live log markers: {liveLogHits}");
        Console.WriteLine($"[live] server alive after live phases: {server.HasExited == false}");
        report.AppendLine($"server alive after live phases: {server.HasExited == false}");

        // Re-check the target's sync state NOW, not just at corruption time. A live server syncs while we
        // observe, so a target that was unsynced when we corrupted it can be synced past by the time we
        // restart - and then recovery skips it unvalidated and phase C reports a meaningless "clean".
        // Without this line that failure mode is invisible.
        if (target != null)
        {
            string syncNow;
            if (File.Exists(journalPath) == false)
            {
                // Not a skip-worthy detail: the corrupted bytes are GONE. Voron reclaims a journal once it is
                // fully synced, so anything that syncs (notably a graceful unload) can delete the evidence
                // outright - after which a clean phase C says nothing about the corruption at all.
                syncNow = "TARGET JOURNAL FILE NO LONGER EXISTS - it was synced and reclaimed, so the corrupted bytes were deleted before restart. Phase C below is NOT a test of this corruption.";
            }
            else
            {
                var envsNow = JournalTools.DiscoverEnvironments(Harness.WorkDir);
                var stillThere = SelectTarget(journalPath, envsNow, ownerFilter, which);
                syncNow = DescribeSyncState(journalPath, target, envsNow);
                if (stillThere == null)
                    syncNow += " | WARNING: the target tx is no longer present in the file";
            }
            Console.WriteLine($"[live] sync state of target BEFORE RESTART: {syncNow}");
            report.AppendLine($"sync state before restart: {syncNow}");
        }

        // ---- Phase C: restart. Confirms the corruption was real and gives a row comparable to the
        // matching at-rest `cell`.
        Console.WriteLine("[live] PHASE C: kill + restart, then verify");
        server.Kill();
        await Task.Delay(1500);

        var consoleOut = Console.Out;
        var capture = new StringWriter();
        Console.SetOut(new DualWriter(consoleOut, capture));
        bool ok;
        try
        {
            ok = await Harness.VerifyAsync(Harness.WorkDir);
        }
        catch (Exception e)
        {
            ok = false;
            Console.WriteLine($"[live] verify threw: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            Console.SetOut(consoleOut);
        }

        RecordLiveFinding(name, op, ownerFilter, which, fileSelector, report.ToString(), capture.ToString());
        Console.WriteLine($"[live] {name} recorded (post-restart verify ok={ok})");
        return 0;
    }

    private static string ArgValue(string[] args, string flag)
    {
        var i = Array.IndexOf(args, flag);
        return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
    }

    private static void ApplyFileLevelOp(string op, string journalPath)
    {
        switch (op)
        {
            case "delete":
                JournalTools.DeleteJournal(journalPath);
                break;
            case "truncate-tail":
                JournalTools.TruncateAt(journalPath, Math.Max(0, new FileInfo(journalPath).Length - 4096));
                break;
            case "diverge":
                JournalTools.DivergeCopy(journalPath, flipByteAt: null);
                break;
        }
    }

    // "already synced" means recovery will skip this tx without validating it, so corrupting it is a
    // no-op finding. Derived from the owner env's on-disk header (LastSyncedJournal / LastSyncedTransactionId).
    private static string DescribeSyncState(string journalPath, JournalTools.TxEntry target, List<JournalTools.EnvInfo> envs)
    {
        var fileName = Path.GetFileNameWithoutExtension(journalPath);
        if (long.TryParse(fileName, out var journalNumber) == false)
            return "<cannot parse journal number>";

        var owner = envs.FirstOrDefault(e => e.Name == target.Owner);
        if (owner?.Header == null)
            return $"journal {journalNumber}, owner '{target.Owner}' has no readable header (link-record or unknown owner)";

        var lastSyncedJournal = owner.Header.Value.Journal.LastSyncedJournal;
        var lastSyncedTx = owner.Header.Value.Journal.LastSyncedTransactionId;
        var alreadySynced = journalNumber < lastSyncedJournal ||
                            (journalNumber == lastSyncedJournal && target.Header.TransactionId <= lastSyncedTx);

        return $"journal {journalNumber}, owner {owner.Name} lastSyncedJournal={lastSyncedJournal} lastSyncedTx={lastSyncedTx}, " +
               $"target txId={target.Header.TransactionId} => {(alreadySynced ? "ALREADY SYNCED (recovery will SKIP it unvalidated - result is meaningless!)" : "unsynced (recovery must replay + validate it)")}";
    }

    private static async Task<int> CountIndexErrorsAsync(Raven.Client.Documents.IDocumentStore store)
    {
        try
        {
            var errors = await store.Maintenance.ForDatabase(Harness.DbName).SendAsync(new GetIndexErrorsOperation());
            return errors.Sum(e => e.Errors.Length);
        }
        catch
        {
            return 0;
        }
    }

    private static async Task<string> SnapshotAsync(Raven.Client.Documents.IDocumentStore store)
    {
        try
        {
            var stats = await store.Maintenance.ForDatabase(Harness.DbName).SendAsync(new GetIndexesStatisticsOperation());
            var errors = await store.Maintenance.ForDatabase(Harness.DbName).SendAsync(new GetIndexErrorsOperation());
            return $"{stats.Length} indexes, errored={stats.Count(s => s.State == IndexState.Error)}, " +
                   $"entries=[{string.Join(", ", stats.OrderBy(s => s.Name).Select(s => $"{s.Name}={s.EntriesCount}"))}], " +
                   $"indexErrors={errors.Sum(e => e.Errors.Length)}";
        }
        catch (Exception e)
        {
            return $"<stats unavailable: {e.GetType().Name}: {e.Message.Split('\n')[0]}>";
        }
    }

    // Watches a running server for signs it noticed the corruption. Detection = the process died, an index
    // flipped to Error, a NEW index error appeared, or the database became unreachable.
    //
    // Deliberately NOT treated as detection: an index reporting entries=0. A RESET index legitimately reads
    // 0 while it rebuilds, so counting that would manufacture a "detection" in phase B every time. It is
    // reported as context only. Index errors are compared against the pre-corruption baseline for the same
    // reason - a historical error carried over from the seed is not evidence of live detection.
    private static async Task<string> ObserveAsync(Raven.Client.Documents.IDocumentStore store, ServerProcess server, int seconds, int baselineErrors)
    {
        var sw = Stopwatch.StartNew();
        var findings = new List<string>();
        var lastSnapshot = "";
        while (sw.Elapsed < TimeSpan.FromSeconds(seconds))
        {
            if (server.HasExited)
            {
                findings.Add($"SERVER PROCESS DIED at t+{sw.Elapsed.TotalSeconds:F0}s (F-3-class regression!)");
                break;
            }
            try
            {
                var stats = await store.Maintenance.ForDatabase(Harness.DbName).SendAsync(new GetIndexesStatisticsOperation());
                var errored = stats.Where(s => s.State == IndexState.Error).Select(s => s.Name).ToList();
                var entries0 = stats.Where(s => s.EntriesCount == 0).Select(s => s.Name).ToList();
                if (errored.Count > 0)
                    findings.Add($"index(es) in Error state at t+{sw.Elapsed.TotalSeconds:F0}s: {string.Join(",", errored)}");

                var errors = await store.Maintenance.ForDatabase(Harness.DbName).SendAsync(new GetIndexErrorsOperation());
                var total = errors.Sum(e => e.Errors.Length);
                if (total > baselineErrors)
                    findings.Add($"{total - baselineErrors} NEW index error(s) at t+{sw.Elapsed.TotalSeconds:F0}s on {errors.Where(e => e.Errors.Length > 0).Select(e => e.Name).FirstOrDefault()}");

                lastSnapshot = $"errored={errored.Count}, indexErrors={total} (baseline {baselineErrors}), entries0=[{string.Join(",", entries0)}]";
            }
            catch (Exception e)
            {
                findings.Add($"database unreachable at t+{sw.Elapsed.TotalSeconds:F0}s: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            }

            if (findings.Count > 0)
                break;
            await Task.Delay(2000);
        }

        return findings.Count == 0
            ? $"NO live detection in {seconds}s (server healthy; {lastSnapshot})"
            : string.Join(" | ", findings.Distinct());
    }

    private static string LiveFindingsFile => Path.Combine(Harness.BaseDir, "findings-scenario1g.md");

    private static void RecordLiveFinding(string name, string op, string owner, string which, string file, string liveReport, string verifyLog)
    {
        if (File.Exists(LiveFindingsFile) == false)
            File.WriteAllText(LiveFindingsFile, "# Scenario 1G findings (corruption while the server is RUNNING)\n\n" +
                "| Cell | Op | Owner | Which | File | Live detection | DB loaded after restart | Result after restart |\n|---|---|---|---|---|---|---|---|\n");

        var loaded = verifyLog.Contains("db loaded") ? "yes" : (verifyLog.Contains("FAILED TO LOAD") ? "NO" : "?");
        var liveDetected = liveReport.Contains("NO live detection") && liveReport.Contains("SERVER PROCESS DIED") == false ? "none" : "SEE DETAIL";
        File.AppendAllText(LiveFindingsFile, $"| {name} | {op} | {owner} | {which} | {file} | {liveDetected} | {loaded} | {Summarize(verifyLog)} |\n");
        File.AppendAllText(LiveFindingsFile, $"\n<!-- {name} live detail:\n{liveReport}\n--- post-restart verify:\n{verifyLog}\n-->\n");
    }

    private static void RecordFinding(string name, string op, string owner, string which, string file, string verifyLog)
    {
        if (File.Exists(FindingsFile) == false)
            File.WriteAllText(FindingsFile, "# Scenario 1 findings (corruption-at-rest)\n\n" +
                "| Cell | Op | Owner | Which | File | DB loaded | Result |\n|---|---|---|---|---|---|---|\n");

        var loaded = verifyLog.Contains("db loaded") ? "yes" : (verifyLog.Contains("FAILED TO LOAD") ? "NO" : "?");
        var summary = Summarize(verifyLog);
        File.AppendAllText(FindingsFile, $"| {name} | {op} | {owner} | {which} | {file} | {loaded} | {summary} |\n");

        // full log appended after the table for detail
        File.AppendAllText(FindingsFile, $"\n<!-- {name} detail:\n{verifyLog}\n-->\n");
    }

    private static string Summarize(string verifyLog)
    {
        var problems = verifyLog.Split('\n')
            .Where(l => l.Contains("PROBLEM") || l.Contains("FAILED TO LOAD") || l.Contains("MISSING") || l.Contains("BELOW BASELINE") || l.Contains("index errors"))
            .Select(l => l.Trim().Replace("|", "/"))
            .Take(3);
        var joined = string.Join("; ", problems);
        return joined.Length == 0 ? "clean recovery" : joined;
    }

    private sealed class DualWriter(TextWriter a, TextWriter b) : TextWriter
    {
        public override Encoding Encoding => a.Encoding;
        public override void Write(char value) { a.Write(value); b.Write(value); }
        public override void Write(string value) { a.Write(value); b.Write(value); }
        public override void WriteLine(string value) { a.WriteLine(value); b.WriteLine(value); }
    }
}
