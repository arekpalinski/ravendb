using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        var txs = JournalTools.Parse(journalPath, envs);
        var matches = txs.Where(t => OwnerMatches(t, ownerFilter)).ToList();
        if (matches.Count == 0)
            return false;

        var target = which switch
        {
            "first" => matches[0],
            "last" => matches[^1],
            _ when which.StartsWith("index:") => matches[int.Parse(which["index:".Length..])],
            _ => throw new ArgumentException($"bad which: {which}")
        };

        Console.WriteLine($"[cell] target tx: {target}");

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
