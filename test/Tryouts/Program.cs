using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Server.Utils;
using SlowTests.Voron.Stress;
using Tests.Infrastructure;

namespace Tryouts;

// Voron v8 Phase III retest tools, picked by the first argument:
//   24520 harness (test/RunBooks/RavenDB-24520): seed | status | map | journal-stats | restore-work | cell | corrupt-live | server | verify | diskfull
//   24528 orchestrator (test/RunBooks/RavenDB-24528): node-info | scenario | carscenario | negative | integrity | numbers-seed | numbers-scenario
//   24514 layouts: 1 | 2 | 3 | 4a | 4b | b1 | b2 | all, upgrade from 7.2: u-swap | u-kill | u-restore
//   stress scenarios: <name> [--seed N] [--minutes M] [--dir path]
public static class Program
{
    private static readonly HashSet<string> Harness24520Commands = new(StringComparer.OrdinalIgnoreCase)
    {
        "seed", "status", "map", "journal-stats", "restore-work", "cell", "corrupt-live", "server", "verify", "diskfull"
    };

    private static readonly HashSet<string> SharedJournals24514Scenarios = new(StringComparer.OrdinalIgnoreCase)
    {
        "1", "2", "3", "4a", "4b", "b1", "b2", "all", "u-swap", "u-kill", "u-restore"
    };

    private static readonly Dictionary<string, Func<StressContext, int>> StressScenarios = new(StringComparer.OrdinalIgnoreCase)
    {
        ["a-order-fuzz"] = AOrderFuzz.Run,
        ["a-fault-matrix"] = AFaultMatrix.Run,
        ["a-merger-pump"] = AMergerPump.RunScenario,
        ["b-recycle-kill"] = BRecycleKill.Run,
        ["b-tail-shapes"] = BTailShapes.Run,
        ["b-prewarm-race"] = BPrewarmRace.Run,
        ["b-shared-kill"] = BSharedKill.Run,
        ["c-reader-soak"] = CReaderSoak.Run,
        ["c-flush-race"] = CFlushRace.Run,
        ["c-snapshot-model"] = CSnapshotModel.Run,
        ["d-page-integrity"] = DPageIntegrity.Run,
        ["d-flusher-liveness"] = DFlusherLiveness.Run,
        ["d-drain-hammer"] = DDrainHammer.Run,
        ["d-avalanche"] = DAvalanche.Run,
        ["e-crash-model"] = ECrashModel.Run,
        ["e-parent-churn"] = EParentChurn.Run,
        ["e-boundary-sweep"] = EBoundarySweep.Run,
        ["f-span-diff"] = FSpanDiff.Run,
        ["f-http-fuzz"] = FHttpFuzz.RunScenario,
        ["f-compare-diff"] = FCompareDiff.Run,
        ["g-cache-assert"] = GCacheAssert.RunScenario,
        ["g-wakeup-watchdog"] = GWakeupWatchdog.RunScenario,
        ["g-etag-restart"] = GEtagRestart.RunScenario,
        ["h-index-stress"] = HIndexStress.Run,
        ["h-tree-model"] = HTreeModel.Run,
        ["h-tree-minimize"] = HTreeMinimize.Run,
        ["h-compressed-churn"] = HCompressedChurn.Run,
        ["h-deep-cursor"] = HDeepCursor.Run,
    };

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: Tryouts <24520 command | 24528 command | 24514 scenario | stress scenario> [options]");
            Console.WriteLine("Stress scenarios: " + string.Join(", ", StressScenarios.Keys));
            return 2;
        }

        if (Harness24520Commands.Contains(args[0]))
        {
            Console.WriteLine($"pid: {Process.GetCurrentProcess().Id}");
            return await RavenDB_24520.Harness.RunAsync(args);
        }

        if (WriteModeOrchestrator.Commands.Contains(args[0]))
            return await WriteModeOrchestrator.RunAsync(args);

        if (SharedJournals24514Scenarios.Contains(args[0]))
            return await RunSharedJournals24514(args[0].ToLowerInvariant());

        if (StressScenarios.ContainsKey(args[0]))
            return RunStress(args);

        Console.WriteLine($"Unknown command '{args[0]}'. Run without arguments for usage.");
        return 2;
    }

    private static int RunStress(string[] args)
    {
        var ctx = StressContext.Parse(args);
        if (ctx.IsChild == false)
            Console.WriteLine($"running {ctx.Scenario}, seed {ctx.Seed}, budget {ctx.Minutes} min, dir {ctx.WorkDir}");

        try
        {
            return StressScenarios[ctx.Scenario](ctx);
        }
        catch (Exception e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e);
            Console.ForegroundColor = ConsoleColor.White;
            return 3;
        }
    }

    private static async Task<int> RunSharedJournals24514(string selector)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        try
        {
            using var testOutputHelper = new ConsoleTestOutputHelper();
            await using var test = new SharedJournals24514(testOutputHelper);

            switch (selector)
            {
                case "1": await test.Scenario1_AllIndexesOnSecondDrive(); break;
                case "2": await test.Scenario2_SomeIndexesOnSecondDrive(); break;
                case "3": await test.Scenario3_SharedJournalsOnSecondDrive(); break;
                case "4a": await test.Scenario4a_DatabaseJournalsOnSecondDrive(); break;
                case "4b": await test.Scenario4b_UpgradeFromV72(); break;
                case "b1": await test.ScenarioB1_SnapshotBackupRestore(); break;
                case "b2": await test.ScenarioB2_BackupFromRelocatedJournals(); break;
                case "u-swap": await test.UpgradeFromV72("swap"); break;
                case "u-kill": await test.UpgradeFromV72("kill"); break;
                case "u-restore": await test.UpgradeFromV72("restore"); break;
                case "all":
                    await test.RunAll();
                    await test.Scenario4b_UpgradeFromV72();
                    break;
            }

            Console.WriteLine("DONE.");
            return 0;
        }
        catch (Exception e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e);
            Console.ForegroundColor = ConsoleColor.White;
            return 1;
        }
    }
}
