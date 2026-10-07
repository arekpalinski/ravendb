using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// f-compare-diff: a cross-branch differential of DocumentCompare.IsEqualTo, the comparer the PR rewrote from
    /// materialized names and boxed values to raw spans. It decides whether a revision is kept, whether a patch is
    /// applied, whether a bulk insert is a no-op and - through ConflictManager.TryResolveIdenticalDocument - whether
    /// replication records a conflict or silently takes the incoming copy.
    ///
    /// Every pair is a random document plus ONE labelled mutation of it (identical, reordered, a changed / added /
    /// removed / re-cased key or value, in the body or in @metadata, a nested change, a number written as a double,
    /// the same content with compressed strings, ...). Each pair is compared in both directions under all four
    /// option sets and one line is printed per comparison. The corpus is fully determined by the seed, so the table
    /// from the PR head and the table from the fork point diff line by line: any line that differs is a behaviour
    /// change of the rewrite, whether or not anyone would call it a bug. Nothing here asserts what the right answer
    /// is - a changed answer is the finding, and pre-existing oddities cancel out.
    ///
    /// The scenario itself only fails if the comparer throws on one side of a pair and not the other direction,
    /// which is independent of the base; the real verdict is the diff.
    /// </summary>
    public static class FCompareDiff
    {
        private const int Pairs = 5000;

        private static readonly (string Name, DocumentCompare.DocumentCompareOptions Options)[] OptionSets =
        [
            ("Default", DocumentCompare.DocumentCompareOptions.Default),
            ("Merge", DocumentCompare.DocumentCompareOptions.MergeMetadata),
            ("MergeThrow", DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModification),
            ("MergeThrowArchival", DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModificationCompareDataArchivalMetadata),
        ];

        private static readonly string[] Kinds =
        [
            "identical", "reordered", "body-value", "body-type", "body-key-case", "body-add", "body-remove",
            "body-at-name", "meta-value", "meta-key-case", "meta-add", "meta-remove", "meta-missing-one-side",
            "nested-value", "array-reorder", "number-as-double", "empty-vs-null", "compressed-strings",
        ];

        // metadata names the comparer special-cases, in several casings, plus names it must ignore
        private static readonly string[] MetadataNames =
        [
            "@collection", "@Collection", "@COLLECTION",
            "@expires", "@Expires", "@EXPIRES",
            "@refresh", "@Refresh",
            "@archive-at", "@Archive-At", "@archived", "@Archived",
            "@counters", "@Counters", "@timeseries", "@TimeSeries", "@attachments", "@Attachments",
            "@flags", "@id", "@last-modified", "@change-vector",
            "@custom", "@x", "Custom", "@metadata"
        ];

        private static readonly string[] BodyNames = ["Name", "name", "Age", "Tags", "Address", "Items", "Note", "Id", "@foo", "a", "A", "aa"];

        public static int Run(StressContext ctx)
        {
            ctx.Iterations = 1;
            var rng = new Random(ctx.Seed);

            Directory.CreateDirectory(ctx.WorkDir);
            var tablePath = Path.Combine(ctx.WorkDir, $"table-f-compare-diff-{ctx.Seed}.txt");
            var pairsPath = Path.Combine(ctx.WorkDir, $"pairs-f-compare-diff-{ctx.Seed}.jsonl");

            var perKind = new Dictionary<string, Dictionary<string, int>>();
            var asymmetric = 0;

            using var table = new StreamWriter(tablePath, append: false, new UTF8Encoding(false));
            using var pairs = new StreamWriter(pairsPath, append: false, new UTF8Encoding(false));
            using var context = JsonOperationContext.ShortTermSingleUse();

            for (var i = 0; i < Pairs; i++)
            {
                var kind = Kinds[rng.Next(Kinds.Length)];
                var a = RandomDocument(rng, needsMetadata: kind.StartsWith("meta-"), needsNested: kind is "nested-value" or "array-reorder");
                var b = Clone(a);
                var modeB = BlittableJsonDocumentBuilder.UsageMode.None;
                Mutate(rng, kind, a, b, ref modeB);

                var aJson = ToJson(a);
                var bJson = ToJson(b);
                pairs.WriteLine($"{{\"i\":{i},\"kind\":\"{kind}\",\"a\":{aJson},\"b\":{bJson},\"bToDisk\":{(modeB == BlittableJsonDocumentBuilder.UsageMode.ToDisk ? "true" : "false")}}}");

                using var left = context.ReadObject(ToDjvObj(a), "a", BlittableJsonDocumentBuilder.UsageMode.None);
                using var right = context.ReadObject(ToDjvObj(b), "b", modeB);

                foreach (var (optName, options) in OptionSets)
                {
                    var forward = Compare(left, right, options);
                    var backward = Compare(right, left, options);
                    table.WriteLine($"{i:D5} {kind,-22} {optName,-18} a->b {forward}");
                    table.WriteLine($"{i:D5} {kind,-22} {optName,-18} b->a {backward}");

                    if (perKind.TryGetValue(kind, out var byResult) == false)
                        perKind[kind] = byResult = new Dictionary<string, int>();
                    byResult[forward] = byResult.GetValueOrDefault(forward) + 1;

                    // equality should not depend on which side is "current": a flip of Equal / NotEqual across the
                    // two directions is worth a line in the report on its own
                    if (Core(forward) != Core(backward))
                        asymmetric++;
                }
            }

            table.Flush();
            pairs.Flush();

            Console.WriteLine($"=== f-compare-diff: {Pairs} pairs x {OptionSets.Length} option sets x 2 directions ===");
            foreach (var (kind, byResult) in perKind.OrderBy(k => k.Key, StringComparer.Ordinal))
                Console.WriteLine($"  {kind,-22} {string.Join("  ", byResult.OrderBy(r => r.Key, StringComparer.Ordinal).Select(r => $"{r.Key}={r.Value}"))}");
            Console.WriteLine($"  comparisons whose Equal/NotEqual answer flips with the argument order: {asymmetric}");
            Console.WriteLine($"table: {tablePath}");
            Console.WriteLine($"pairs: {pairsPath}");
            Console.WriteLine("diff the table against a run of the same seed on the fork point - every differing line is a behaviour change");

            return ctx.Finish();
        }

        private static string Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y, DocumentCompare.DocumentCompareOptions options)
        {
            try
            {
                return DocumentCompare.IsEqualTo(x, y, options).ToString().Replace(", ", "|");
            }
            catch (Exception e)
            {
                return "throw:" + e.GetType().Name;
            }
        }

        private static string Core(string result) =>
            result.StartsWith("throw:", StringComparison.Ordinal) ? result
            : result.Contains("Equal", StringComparison.Ordinal) && result.Contains("NotEqual", StringComparison.Ordinal) == false ? "Equal"
            : result.Split('|')[0];

        // ---- the document model: objects keep insertion order, so a reorder is a real reorder ----

        private sealed class JObj : List<KeyValuePair<string, object>>
        {
            public int IndexOf(string key) => FindIndex(p => p.Key == key);
        }

        private sealed class JArr : List<object>
        {
        }

        private static JObj RandomDocument(Random rng, bool needsMetadata, bool needsNested)
        {
            var doc = new JObj();
            var count = 1 + rng.Next(5);
            foreach (var name in BodyNames.OrderBy(_ => rng.Next()).Take(count))
                doc.Add(new(name, RandomValue(rng, depth: 1)));

            if (needsNested)
            {
                doc.Add(new("Nested", new JObj { new("City", "Hadera"), new("Zip", 12345L), new("Tags", new JArr { "x", "y", 3L }) }));
                doc.Add(new("List", new JArr { 1L, 2L, "three", new JObj { new("k", "v") } }));
            }

            if (needsMetadata || rng.Next(3) != 0)
            {
                var meta = new JObj { new("@collection", "Users") };
                foreach (var name in MetadataNames.Where(n => n != "@collection").OrderBy(_ => rng.Next()).Take(rng.Next(4)))
                    meta.Add(new(name, MetadataValue(rng, name)));
                doc.Add(new("@metadata", meta));
            }

            return doc;
        }

        private static object MetadataValue(Random rng, string name)
        {
            switch (name.ToLowerInvariant())
            {
                case "@counters":
                case "@timeseries":
                    return new JArr { "c" + rng.Next(3), "d" + rng.Next(3) };
                case "@attachments":
                    return new JArr
                    {
                        new JObj { new("Name", "a" + rng.Next(3)), new("Hash", "h" + rng.Next(3)), new("ContentType", "text/plain"), new("Size", (long)rng.Next(100)) }
                    };
                case "@archived":
                    return rng.Next(2) == 0;
                default:
                    return "2030-0" + (1 + rng.Next(9)) + "-01T00:00:00.0000000Z";
            }
        }

        private static object RandomValue(Random rng, int depth)
        {
            switch (rng.Next(depth > 2 ? 6 : 8))
            {
                case 0: return (long)rng.Next(-1000, 1000);
                case 1: return rng.Next(100) / 4.0 + 0.5;
                case 2: return "s" + rng.Next(100);
                case 3: return new string((char)('a' + rng.Next(26)), 150 + rng.Next(200)); // long enough to be compressed ToDisk
                case 4: return rng.Next(2) == 0;
                case 5: return null;
                case 6: return new JObj { new("k" + rng.Next(3), RandomValue(rng, depth + 1)), new("z", RandomValue(rng, depth + 1)) };
                default: return new JArr { RandomValue(rng, depth + 1), RandomValue(rng, depth + 1) };
            }
        }

        private static object Clone(object value) => value switch
        {
            JObj o => CloneObj(o),
            JArr a => CloneArr(a),
            _ => value
        };

        private static JObj CloneObj(JObj o)
        {
            var copy = new JObj();
            foreach (var p in o)
                copy.Add(new(p.Key, Clone(p.Value)));
            return copy;
        }

        private static JArr CloneArr(JArr a)
        {
            var copy = new JArr();
            copy.AddRange(a.Select(x => Clone(x)));
            return copy;
        }

        private static JObj Clone(JObj o) => CloneObj(o);

        private static void Mutate(Random rng, string kind, JObj a, JObj b, ref BlittableJsonDocumentBuilder.UsageMode modeB)
        {
            var bodyKeys = b.Where(p => p.Key != "@metadata").Select(p => p.Key).ToList();
            var meta = b.IndexOf("@metadata") is var mi and >= 0 ? (JObj)b[mi].Value : null;

            switch (kind)
            {
                case "identical":
                    return;

                case "reordered":
                    Reorder(rng, b);
                    return;

                case "body-value":
                {
                    var key = bodyKeys[rng.Next(bodyKeys.Count)];
                    Set(b, key, "changed-" + rng.Next(1000));
                    return;
                }

                case "body-type":
                {
                    var key = bodyKeys[rng.Next(bodyKeys.Count)];
                    var current = b[b.IndexOf(key)].Value;
                    Set(b, key, current switch
                    {
                        long l => l.ToString(),
                        double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        bool x => x ? "true" : "false",
                        null => "null",
                        string s => s.Length,
                        _ => "container"
                    });
                    return;
                }

                case "body-key-case":
                {
                    var key = bodyKeys[rng.Next(bodyKeys.Count)];
                    var renamed = key.Any(char.IsLower) ? key.ToUpperInvariant() : key.ToLowerInvariant();
                    if (renamed == key)
                        renamed = key + "x";
                    if (b.IndexOf(renamed) >= 0)
                        b.RemoveAt(b.IndexOf(renamed));
                    var idx = b.IndexOf(key);
                    b[idx] = new(renamed, b[idx].Value);
                    return;
                }

                case "body-add":
                    b.Insert(0, new("Added" + rng.Next(10), RandomValue(rng, 1)));
                    return;

                case "body-remove":
                    b.RemoveAt(b.IndexOf(bodyKeys[rng.Next(bodyKeys.Count)]));
                    return;

                case "body-at-name":
                {
                    // an @-prefixed property in the BODY: the comparer skips @-names only inside @metadata... or does it
                    var name = rng.Next(2) == 0 ? "@foo" : "@Metadata";
                    if (b.IndexOf(name) < 0)
                        b.Insert(0, new(name, "v" + rng.Next(10)));
                    else
                        Set(b, name, "other");
                    return;
                }

                case "meta-value":
                {
                    var key = meta[rng.Next(meta.Count)].Key;
                    var current = meta[meta.IndexOf(key)].Value;
                    Set(meta, key, current is string s ? s.Replace("2030", "2041") : MetadataValue(new Random(rng.Next()), key));
                    return;
                }

                case "meta-key-case":
                {
                    var key = meta[rng.Next(meta.Count)].Key;
                    var renamed = key.Length > 1 && char.IsLower(key[1]) ? "@" + char.ToUpperInvariant(key[1]) + key[2..] : key.ToLowerInvariant();
                    if (renamed == key)
                        renamed = key.ToUpperInvariant();
                    if (meta.IndexOf(renamed) >= 0)
                        meta.RemoveAt(meta.IndexOf(renamed));
                    var idx = meta.IndexOf(key);
                    meta[idx] = new(renamed, meta[idx].Value);
                    return;
                }

                case "meta-add":
                {
                    var name = MetadataNames[rng.Next(MetadataNames.Length)];
                    if (meta.IndexOf(name) < 0)
                        meta.Add(new(name, MetadataValue(rng, name)));
                    else
                        Set(meta, name, MetadataValue(new Random(rng.Next() + 1), name));
                    return;
                }

                case "meta-remove":
                    meta.RemoveAt(rng.Next(meta.Count));
                    return;

                case "meta-missing-one-side":
                    b.RemoveAt(b.IndexOf("@metadata"));
                    return;

                case "nested-value":
                {
                    var nested = (JObj)b[b.IndexOf("Nested")].Value;
                    Set(nested, "City", rng.Next(2) == 0 ? "hadera" : "Haifa");
                    return;
                }

                case "array-reorder":
                {
                    var list = (JArr)b[b.IndexOf("List")].Value;
                    list.Reverse();
                    return;
                }

                case "number-as-double":
                {
                    var key = bodyKeys.FirstOrDefault(k => b[b.IndexOf(k)].Value is long);
                    if (key == null)
                    {
                        a.Insert(0, new("N", 100L));
                        b.Insert(0, new("N", 100.0));
                    }
                    else
                    {
                        Set(b, key, (double)(long)b[b.IndexOf(key)].Value);
                    }
                    return;
                }

                case "empty-vs-null":
                {
                    a.Insert(0, new("E", ""));
                    b.Insert(0, new("E", null));
                    return;
                }

                case "compressed-strings":
                    modeB = BlittableJsonDocumentBuilder.UsageMode.ToDisk;
                    return;

                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
            }
        }

        private static void Set(JObj o, string key, object value)
        {
            var idx = o.IndexOf(key);
            o[idx] = new(key, value);
        }

        private static void Reorder(Random rng, JObj o)
        {
            var shuffled = o.OrderBy(_ => rng.Next()).ToList();
            o.Clear();
            foreach (var p in shuffled)
            {
                if (p.Value is JObj inner)
                    Reorder(rng, inner);
                o.Add(p);
            }
        }

        private static object ToDjv(object value) => value switch
        {
            JObj o => ToDjvObj(o),
            JArr a => new DynamicJsonArray(a.Select(x => ToDjv(x))),
            _ => value
        };

        private static DynamicJsonValue ToDjvObj(JObj o)
        {
            var djv = new DynamicJsonValue();
            foreach (var p in o)
                djv[p.Key] = ToDjv(p.Value);
            return djv;
        }

        private static string ToJson(object value)
        {
            var sb = new StringBuilder();
            WriteJson(sb, value);
            return sb.ToString();
        }

        private static void WriteJson(StringBuilder sb, object value)
        {
            switch (value)
            {
                case null: sb.Append("null"); break;
                case bool x: sb.Append(x ? "true" : "false"); break;
                case long l: sb.Append(l); break;
                case double d: sb.Append(d.ToString("R", System.Globalization.CultureInfo.InvariantCulture)); if (d == Math.Floor(d)) sb.Append(".0"); break;
                case string s: sb.Append('"').Append(s.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"'); break;
                case JArr a:
                    sb.Append('[');
                    for (var i = 0; i < a.Count; i++)
                    {
                        if (i > 0) sb.Append(',');
                        WriteJson(sb, a[i]);
                    }
                    sb.Append(']');
                    break;
                case JObj o:
                    sb.Append('{');
                    for (var i = 0; i < o.Count; i++)
                    {
                        if (i > 0) sb.Append(',');
                        sb.Append('"').Append(o[i].Key).Append("\":");
                        WriteJson(sb, o[i].Value);
                    }
                    sb.Append('}');
                    break;
                default: sb.Append('"').Append(value).Append('"'); break;
            }
        }
    }
}
