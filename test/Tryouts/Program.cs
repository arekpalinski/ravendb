using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine($"pid: {Process.GetCurrentProcess().Id}");
        return await RavenDB_24520.Harness.RunAsync(args);
    }
}
