using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using FastTests;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// f-http-fuzz: the PR rewrote several readers that sit directly on user-supplied HTTP bodies, and
    /// changed them from "skip what you cannot read" to "convert it". JsonDeserializationBase's dictionary
    /// converters now call ConvertType / Enum.Parse / a direct cast instead of TryGet-and-continue;
    /// BatchRequestParser evaluates CollectionName.GetCollectionName during request parsing; MultiGet casts
    /// Requests elements and Content without a guard. FormatException, InvalidCastException,
    /// ArgumentException and NullReferenceException are all absent from RavenServerStartup's
    /// exception-to-status map, so any of them is a 500 on an endpoint a client can reach.
    ///
    /// The scenario is a deterministic differential sweep, not a random fuzz: a fixed set of probes
    /// (endpoint + JSON template with one hole) crossed with a fixed set of mutations. It prints one
    /// sorted line per (probe, mutation) with the resulting status and exception type, so a run on the PR
    /// head and a run on the merge-base diff cleanly. A probe answering >= 500 is a failure on its own.
    /// </summary>
    public class FHttpFuzz(ITestOutputHelper output) : RavenTestBase(output)
    {
        public static int RunScenario(StressContext ctx) => RunScenarioAsync(ctx).GetAwaiter().GetResult();

        private static async Task<int> RunScenarioAsync(StressContext ctx)
        {
            using var output = new ConsoleTestOutputHelper();
            await using var test = new FHttpFuzz(output);
            return await test.RunAsync(ctx);
        }

        // One JSON value substituted into a probe's hole. Names are stable so two runs diff line by line.
        private static readonly (string Name, string Json)[] Mutations =
        [
            ("valid", null), // replaced per probe by its own valid value
            ("string", "\"a string\""),
            ("emptystring", "\"\""),
            ("int", "12345"),
            ("double", "12.5"),
            ("bool", "true"),
            ("null", "null"),
            ("emptyobject", "{}"),
            ("object", "{\"nested\":1}"),
            ("emptyarray", "[]"),
            ("intarray", "[1,2,3]"),
            ("stringarray", "[\"a\",\"b\"]"),
            ("nestedarray", "[[1],[2]]"),
            ("objectarray", "[{\"a\":1}]"),
            ("unknownenum", "\"NotAStatus\""),
            ("hugestring", "\"x\""), // expanded to 64KB by Expand, kept short here so the table stays readable
        ];

        private sealed class Probe
        {
            public string Name;
            public string Method;
            public Func<string, string> Url;     // takes the mutation name, so a probe can vary the url
            public Func<string, string> Body;    // takes the mutation JSON
            public string ValidJson;             // what "valid" substitutes
            public string Why;                   // what the probe is aimed at
        }

        private async Task<int> RunAsync(StressContext ctx)
        {
            ctx.Iterations = 1;

            using var store = GetDocumentStore();
            using var client = new HttpClient();

            var probes = BuildProbes(store.Urls[0], store.Database);

            var rows = new List<string>();
            var failures = 0;
            var sent = 0;

            foreach (var probe in probes)
            {
                foreach (var (name, json) in Mutations)
                {
                    var value = name == "valid" ? probe.ValidJson : Expand(json);
                    var body = probe.Body(value);
                    var url = probe.Url(name);

                    string outcome;
                    try
                    {
                        using var request = new HttpRequestMessage(new HttpMethod(probe.Method), url)
                        {
                            Content = new StringContent(body, Encoding.UTF8, "application/json")
                        };
                        using var response = await client.SendAsync(request);
                        var content = await response.Content.ReadAsStringAsync();

                        outcome = Classify(probe, response.StatusCode, content, out var isFailure);
                        if (DumpEverything)
                            WriteEvidence(ctx, probe, name, body, (int)response.StatusCode, content);
                        if (isFailure)
                        {
                            failures++;
                            WriteEvidence(ctx, probe, name, body, (int)response.StatusCode, content);
                        }
                    }
                    catch (Exception e)
                    {
                        outcome = $"transport:{e.GetType().Name}";
                        failures++;
                        WriteEvidence(ctx, probe, name, body, -1, e.ToString());
                    }

                    rows.Add($"{probe.Name,-28} {name,-14} {outcome}");
                    sent++;

                    if (sent % 25 == 0 && await ServerIsHealthy(store) == false)
                    {
                        ctx.Fail($"the server stopped answering GetStatisticsOperation after {sent} requests (last: {probe.Name}/{name})");
                        Report(ctx, rows, sent, failures);
                        return ctx.Finish();
                    }
                }
            }

            Report(ctx, rows, sent, failures);

            if (failures > 0)
                ctx.Fail($"{failures} of {sent} requests produced a server error (5xx, a 500 inside a multi_get envelope, or a transport failure) - see {Path.Combine(ctx.WorkDir, "failures")}");

            return ctx.Finish();
        }

        // VORON_FUZZ_DUMP=1 keeps the response of every request, not just the failing ones
        private static readonly bool DumpEverything = Environment.GetEnvironmentVariable("VORON_FUZZ_DUMP") == "1";

        private static string Expand(string json)
        {
            if (json == null)
                return null;

            // the hugestring entry is written short above so the table stays readable
            if (json == "\"x\"")
                return "\"" + new string('x', 64 * 1024) + "\"";

            return json;
        }

        private static List<Probe> BuildProbes(string serverUrl, string database)
        {
            var db = $"{serverUrl}/databases/{database}";

            return
            [
                // ---- JsonDeserializationBase dictionary converters, reached through PUT /admin/databases ----
                // DeletionInProgress is Dictionary<string, DeletionInProgressStatus> -> ToDictionaryOfEnum,
                // which now does ConvertType(value, out string) + Enum.Parse. The database name is the one
                // that already exists, so a body that DOES deserialize is rejected cleanly by the create
                // path instead of leaving a database behind.
                new Probe
                {
                    Name = "admin-databases/DeletionInProgress",
                    Method = "PUT",
                    Url = _ => $"{serverUrl}/admin/databases?name={database}&replicationFactor=1",
                    ValidJson = "\"No\"",
                    Why = "ToDictionaryOfEnum: ConvertType + Enum.Parse on a user-supplied value",
                    Body = v => "{\"DatabaseName\":\"" + database + "\",\"DeletionInProgress\":{\"A\":" + v + "}}"
                },
                new Probe
                {
                    Name = "admin-databases/DeletionInProgress-nested",
                    Method = "PUT",
                    Url = _ => $"{serverUrl}/admin/databases?name={database}&replicationFactor=1",
                    ValidJson = "\"SoftDelete\"",
                    Why = "ToDictionaryOfEnum: the value nested one level deeper",
                    Body = v => "{\"DatabaseName\":\"" + database + "\",\"DeletionInProgress\":{\"A\":{\"nested\":" + v + "}}}"
                },
                // IndexesHistory is Dictionary<string, List<IndexHistoryEntry>> -> ToDictionaryOfList, which
                // does ConvertType(value, out BlittableJsonReaderArray) and then dereferences array.Length.
                new Probe
                {
                    Name = "admin-databases/IndexesHistory",
                    Method = "PUT",
                    Url = _ => $"{serverUrl}/admin/databases?name={database}&replicationFactor=1",
                    ValidJson = "[]",
                    Why = "ToDictionaryOfList: ConvertType to an array then .Length",
                    Body = v => "{\"DatabaseName\":\"" + database + "\",\"IndexesHistory\":{\"Idx\":" + v + "}}"
                },
                new Probe
                {
                    Name = "admin-databases/Settings",
                    Method = "PUT",
                    Url = _ => $"{serverUrl}/admin/databases?name={database}&replicationFactor=1",
                    ValidJson = "\"value\"",
                    Why = "ToDictionaryOfString (the lenient one) - control probe",
                    Body = v => "{\"DatabaseName\":\"" + database + "\",\"Settings\":{\"Some.Key\":" + v + "}}"
                },

                // ---- BatchRequestParser: the PR moved CollectionName.GetCollectionName onto the HTTP thread ----
                new Probe
                {
                    Name = "bulk_docs/@metadata",
                    Method = "POST",
                    Url = _ => $"{db}/bulk_docs",
                    ValidJson = "{\"@collection\":\"Fuzz\"}",
                    Why = "GetCollectionName now runs during request parsing (PR change)",
                    Body = v => "{\"Commands\":[{\"Type\":\"PUT\",\"Id\":\"fuzz/1\",\"Document\":{\"Name\":\"x\",\"@metadata\":" + v + "}}]}"
                },
                new Probe
                {
                    Name = "bulk_docs/@collection",
                    Method = "POST",
                    Url = _ => $"{db}/bulk_docs",
                    ValidJson = "\"Fuzz\"",
                    Why = "the collection name itself, read on the HTTP thread",
                    Body = v => "{\"Commands\":[{\"Type\":\"PUT\",\"Id\":\"fuzz/2\",\"Document\":{\"Name\":\"x\",\"@metadata\":{\"@collection\":" + v + "}}}]}"
                },
                new Probe
                {
                    Name = "bulk_docs/Document",
                    Method = "POST",
                    Url = _ => $"{db}/bulk_docs",
                    ValidJson = "{\"Name\":\"x\"}",
                    Why = "the document itself is not an object",
                    Body = v => "{\"Commands\":[{\"Type\":\"PUT\",\"Id\":\"fuzz/3\",\"Document\":" + v + "}]}"
                },
                new Probe
                {
                    Name = "bulk_docs/Commands",
                    Method = "POST",
                    Url = _ => $"{db}/bulk_docs",
                    ValidJson = "[]",
                    Why = "the command array itself",
                    Body = v => "{\"Commands\":" + v + "}"
                },

                // ---- MultiGet: unguarded casts at the element and Content level ----
                new Probe
                {
                    Name = "multi_get/element",
                    Method = "POST",
                    Url = _ => $"{db}/multi_get",
                    ValidJson = "{\"Url\":\"/databases/" + database + "/docs\",\"Query\":\"?id=users/1\",\"Method\":\"GET\"}",
                    Why = "(BlittableJsonReaderObject)requests[i] is cast outside the per-request try",
                    Body = v => "{\"Requests\":[" + v + "]}"
                },
                new Probe
                {
                    Name = "multi_get/Content",
                    Method = "POST",
                    Url = _ => $"{db}/multi_get",
                    ValidJson = "{\"Query\":\"from Users\"}",
                    Why = "(BlittableJsonReaderObject)content on a POST sub-request",
                    Body = v => "{\"Requests\":[{\"Url\":\"/databases/" + database + "/queries\",\"Query\":\"?\",\"Method\":\"POST\",\"Content\":" + v + "}]}"
                },
                new Probe
                {
                    Name = "multi_get/Headers",
                    Method = "POST",
                    Url = _ => $"{db}/multi_get",
                    ValidJson = "{\"X-Probe\":\"v\"}",
                    Why = "ConvertType on every header value (already measured by MultiGetWrongTypedHeader)",
                    Body = v => "{\"Requests\":[{\"Url\":\"/databases/" + database + "/docs\",\"Query\":\"?id=users/1\",\"Method\":\"GET\",\"Headers\":" + v + "}]}"
                },
                new Probe
                {
                    Name = "multi_get/HeaderValue",
                    Method = "POST",
                    Url = _ => $"{db}/multi_get",
                    ValidJson = "\"v\"",
                    Why = "ConvertType(prop.Value, out string) on a single header VALUE - the f-2 claim",
                    Body = v => "{\"Requests\":[{\"Url\":\"/databases/" + database + "/docs\",\"Query\":\"?id=users/1\",\"Method\":\"GET\",\"Headers\":{\"X-Probe\":" + v + "}}]}"
                },
                new Probe
                {
                    Name = "multi_get/Requests",
                    Method = "POST",
                    Url = _ => $"{db}/multi_get",
                    ValidJson = "[]",
                    Why = "the Requests property itself",
                    Body = v => "{\"Requests\":" + v + "}"
                }
            ];
        }

        private static readonly Regex ExceptionType = new("\"Type\"\\s*:\\s*\"([^\"]+)\"", RegexOptions.Compiled);
        private static readonly Regex SubStatus = new("\"StatusCode\"\\s*:\\s*(\\d+)", RegexOptions.Compiled);

        private static string Classify(Probe probe, HttpStatusCode status, string content, out bool isFailure)
        {
            var code = (int)status;
            var type = ExceptionType.Match(content) is { Success: true } m ? Shorten(m.Groups[1].Value) : "-";

            // multi_get answers 200 at the envelope level and carries per-request status inside
            if (probe.Name.StartsWith("multi_get", StringComparison.Ordinal) && code == 200)
            {
                var worst = SubStatus.Matches(content).Select(x => int.Parse(x.Groups[1].Value)).DefaultIfEmpty(200).Max();
                isFailure = worst >= 500;
                return $"200 (inner {worst}) {type}";
            }

            isFailure = code >= 500;
            return $"{code} {type}";
        }

        private static string Shorten(string typeName)
        {
            var dot = typeName.LastIndexOf('.');
            return dot < 0 ? typeName : typeName[(dot + 1)..];
        }

        private async Task<bool> ServerIsHealthy(Raven.Client.Documents.IDocumentStore store)
        {
            try
            {
                await store.Maintenance.SendAsync(new GetStatisticsOperation());
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"  health check failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
                return false;
            }
        }

        private static void WriteEvidence(StressContext ctx, Probe probe, string mutation, string body, int status, string response)
        {
            var dir = Path.Combine(ctx.WorkDir, "failures");
            Directory.CreateDirectory(dir);
            var safe = probe.Name.Replace('/', '_');
            var path = Path.Combine(dir, $"{safe}-{mutation}.txt");
            File.WriteAllText(path,
                $"probe: {probe.Name}{Environment.NewLine}" +
                $"why:   {probe.Why}{Environment.NewLine}" +
                $"{probe.Method} {probe.Url(mutation)}{Environment.NewLine}{Environment.NewLine}" +
                $"request body:{Environment.NewLine}{Truncate(body)}{Environment.NewLine}{Environment.NewLine}" +
                $"status: {status}{Environment.NewLine}{Environment.NewLine}" +
                $"response:{Environment.NewLine}{Truncate(response)}{Environment.NewLine}");
        }

        private static string Truncate(string s) => s.Length <= 4000 ? s : s[..4000] + $"... ({s.Length} chars)";

        private static void Report(StressContext ctx, List<string> rows, int sent, int failures)
        {
            Console.WriteLine();
            Console.WriteLine($"=== f-http-fuzz: {sent} requests, {failures} server errors ===");
            Console.WriteLine("(diff this table against a run on the merge-base to attribute anything to the PR)");
            foreach (var row in rows)
                Console.WriteLine(row);

            Directory.CreateDirectory(ctx.WorkDir);
            var table = Path.Combine(ctx.WorkDir, $"table-f-http-fuzz-{ctx.Seed}.txt");
            File.WriteAllLines(table, rows);
            Console.WriteLine($"table written to {table}");
        }
    }
}
