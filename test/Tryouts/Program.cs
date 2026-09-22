using System;
using System.Collections.Generic;
using SlowTests.Voron.Stress;

namespace Tryouts;

public static class Program
{
    // PR 23438 stress campaign dispatcher.
    // Usage: Tryouts <scenario> [--seed N] [--minutes M] [--dir path]
    // Child mode (internal, spawned by crash scenarios): Tryouts <scenario> --child --seed N --iteration I --dir path
    private static readonly Dictionary<string, Func<StressContext, int>> Scenarios = new(StringComparer.OrdinalIgnoreCase)
    {
        ["a-order-fuzz"] = AOrderFuzz.Run,
        ["a-fault-matrix"] = AFaultMatrix.Run,
        ["a-merger-pump"] = AMergerPump.RunScenario,
        ["b-recycle-kill"] = BRecycleKill.Run,
        ["b-tail-shapes"] = BTailShapes.Run,
        ["b-prewarm-race"] = BPrewarmRace.Run,
        ["c-reader-soak"] = CReaderSoak.Run,
        ["c-flush-race"] = CFlushRace.Run,
        ["c-snapshot-model"] = CSnapshotModel.Run,
        ["d-page-integrity"] = DPageIntegrity.Run,
        ["d-flusher-liveness"] = DFlusherLiveness.Run,
        ["d-drain-hammer"] = DDrainHammer.Run,
        ["e-crash-model"] = ECrashModel.Run,
        ["e-parent-churn"] = EParentChurn.Run,
        ["e-boundary-sweep"] = EBoundarySweep.Run,
        ["f-span-diff"] = FSpanDiff.Run,
        ["f-http-fuzz"] = FHttpFuzz.RunScenario,
        ["g-cache-assert"] = GCacheAssert.RunScenario,
        ["g-wakeup-watchdog"] = GWakeupWatchdog.RunScenario,
        ["h-index-stress"] = HIndexStress.Run,
        ["h-tree-model"] = HTreeModel.Run,
        ["h-tree-minimize"] = HTreeMinimize.Run,
        ["h-compressed-churn"] = HCompressedChurn.Run,
        ["h-deep-cursor"] = HDeepCursor.Run,
    };

    public static int Main(string[] args)
    {
        if (args.Length == 0 || Scenarios.ContainsKey(args[0]) == false)
        {
            Console.WriteLine("Usage: Tryouts <scenario> [--seed N] [--minutes M] [--dir path]");
            Console.WriteLine("Scenarios: " + string.Join(", ", Scenarios.Keys));
            return 2;
        }

        var ctx = StressContext.Parse(args);
        if (ctx.IsChild == false)
            Console.WriteLine($"running {ctx.Scenario}, seed {ctx.Seed}, budget {ctx.Minutes} min, dir {ctx.WorkDir}");

        try
        {
            return Scenarios[ctx.Scenario](ctx);
        }
        catch (Exception e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e);
            Console.ForegroundColor = ConsoleColor.White;
            return 3;
        }
    }
}
