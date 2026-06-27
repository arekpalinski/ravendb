#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        // XunitLogging removed in xUnit v3 migration
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        // RavenDB-24514 - Shared Journals junction / different-drive scenarios.
        // Usage: dotnet run --project test/Tryouts -c Release -- [1|2|3|4a|4b|all]   (default: 1,2,3,4a)
        string selector = args.Length > 0 ? args[0].ToLowerInvariant() : "default";

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
                case "all":
                    await test.RunAll();
                    await test.Scenario4b_UpgradeFromV72();
                    break;
                default: await test.RunAll(); break;
            }

            Console.WriteLine("DONE.");
        }
        catch (Exception e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e);
            Console.ForegroundColor = ConsoleColor.White;
        }
    }

    private static void TryRemoveDatabasesFolder()
    {
        string p = AppDomain.CurrentDomain.BaseDirectory;
        string dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
    }
}
