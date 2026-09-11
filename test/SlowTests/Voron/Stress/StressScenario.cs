using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// Shared plumbing for the PR-23438 stress campaign. Scenarios are console-driven from the
    /// Tryouts project: `Tryouts <scenario> --seed N --minutes M [--dir path] [--child --iteration I]`.
    /// A crash scenario runs its workload in a child process (same executable, --child) that the
    /// parent kills at a seeded random moment, then verifies recovery in-process.
    /// </summary>
    public sealed class StressContext
    {
        public string Scenario;
        public int Seed;
        public double Minutes = 15;
        public string WorkDir;
        public bool IsChild;
        public int Iteration;

        public readonly Stopwatch Clock = Stopwatch.StartNew();
        public readonly List<string> Failures = new();
        public int Iterations;
        public int VacuousIterations; // child was killed before committing anything - valid, but all-vacuous means no coverage

        public bool TimeLeft => Clock.Elapsed.TotalMinutes < Minutes;

        public string IterationDir(int iteration) => Path.Combine(WorkDir, $"iter-{iteration:D4}");

        public static StressContext Parse(string[] args)
        {
            var ctx = new StressContext { Scenario = args[0] };
            for (var i = 1; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--seed":
                        ctx.Seed = int.Parse(args[++i]);
                        break;
                    case "--minutes":
                        ctx.Minutes = double.Parse(args[++i]);
                        break;
                    case "--dir":
                        ctx.WorkDir = args[++i];
                        break;
                    case "--child":
                        ctx.IsChild = true;
                        break;
                    case "--iteration":
                        ctx.Iteration = int.Parse(args[++i]);
                        break;
                    default:
                        throw new ArgumentException($"Unknown argument '{args[i]}'");
                }
            }

            if (ctx.Seed == 0)
                ctx.Seed = Environment.TickCount;
            ctx.WorkDir ??= Path.Combine(Path.GetTempPath(), "voron-stress", $"{ctx.Scenario}-{ctx.Seed}");
            return ctx;
        }

        public void Fail(string message)
        {
            Failures.Add(message);
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"FAIL [{Scenario} seed {Seed} iter {Iteration}]: {message}");
            Console.ForegroundColor = ConsoleColor.White;
        }

        /// <summary>
        /// Spawns this executable as `scenario --child` on the given iteration directory, waits a
        /// seeded random slice of the child's lifetime, kills the process tree, and waits for exit.
        /// Returns the child's runtime in ms.
        /// </summary>
        public long RunChildAndKill(int iteration, Random rng, int minLifetimeMs = 1500, int maxLifetimeMs = 8000)
        {
            var psi = new ProcessStartInfo
            {
                FileName = Environment.ProcessPath,
                ArgumentList =
                {
                    Scenario, "--child",
                    "--seed", Seed.ToString(),
                    "--iteration", iteration.ToString(),
                    "--dir", WorkDir
                },
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            using var child = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start the child process");
            child.OutputDataReceived += (_, e) => { if (e.Data != null) Console.WriteLine($"  child> {e.Data}"); };
            child.ErrorDataReceived += (_, e) => { if (e.Data != null) Console.WriteLine($"  child! {e.Data}"); };
            child.BeginOutputReadLine();
            child.BeginErrorReadLine();

            var lifetime = rng.Next(minLifetimeMs, maxLifetimeMs);
            var sp = Stopwatch.StartNew();
            if (child.WaitForExit(lifetime) == false)
            {
                try
                {
                    child.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                    // exited between the check and the kill
                }
            }

            child.WaitForExit();
            return sp.ElapsedMilliseconds;
        }

        public int Finish()
        {
            if (Iterations > 0 && VacuousIterations == Iterations)
                Failures.Add($"all {Iterations} iterations were vacuous (the child never committed before the kill) - no coverage");

            var verdict = Failures.Count == 0 ? "PASS" : "FAIL";
            var result = new
            {
                Scenario,
                Seed,
                Iterations,
                Elapsed = Clock.Elapsed.ToString(),
                Verdict = verdict,
                Failures
            };

            Directory.CreateDirectory(WorkDir);
            var path = Path.Combine(WorkDir, $"results-{Scenario}-{Seed}.json");
            File.WriteAllText(path, JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));

            Console.WriteLine($"{verdict} [{Scenario}] seed {Seed}, {Iterations} iterations in {Clock.Elapsed}. Results: {path}");
            if (Failures.Count > 0)
                Console.WriteLine($"Evidence kept under {WorkDir} - the run is replayable with the same seed.");

            return Failures.Count == 0 ? 0 : 1;
        }
    }
}
