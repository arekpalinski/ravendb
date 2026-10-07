using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace SlowTests.Voron.Stress
{
    /// <summary>
    /// f-span-diff: differential fuzz of the span-based blittable property access through its main
    /// consumer, DocumentCompare.IsEqualTo (GetPropertyIndex(span), TryCompareValuesByIndex,
    /// IsEqualConstant), cross-checked against BlittableJsonReaderObject.Equals. Documents are built
    /// from adversarial property names (case variants, shared prefixes, unicode, escapes, digits,
    /// 300-char names, '@'-prefixed non-metadata names, metadata keys) and all value kinds (strings
    /// short/long/compressible, integers, doubles, bools, nulls, nested objects, arrays). Per document:
    ///   1. reflexive: IsEqualTo(D, D) is Equal;
    ///   2. insertion-order invariance: the same content with shuffled property order (recursively) is Equal;
    ///   3. representation invariance: the same content built with string compression on/off is Equal;
    ///   4. mutation detection: one changed/removed/case-renamed non-metadata property is NotEqual;
    ///      inside @metadata only the keys DocumentCompare treats as significant must be detected;
    ///   5. the two implementations agree on every pair (for documents without @metadata);
    ///   6. no exception escapes on any input.
    /// </summary>
    public static class FSpanDiff
    {
        private static readonly string[] SignificantMetadata =
        {
            "@collection", "@expires", "@refresh", "@archive-at", "@archived", "@attachments", "@counters", "@timeseries"
        };

        public static int Run(StressContext ctx)
        {
            var rng = new Random(ctx.Seed);
            var names = BuildNamePool();
            long docs = 0, pairs = 0;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                while (ctx.TimeLeft)
                {
                    var withMetadata = rng.Next(3) == 0;
                    var model = RandomObject(rng, names, depth: 0, withMetadata);
                    docs++;

                    try
                    {
                        using var plain = context.ReadObject(ToDjv(model), "d", BlittableJsonDocumentBuilder.UsageMode.None);
                        using var compressed = context.ReadObject(ToDjv(model), "d", BlittableJsonDocumentBuilder.UsageMode.ToDisk); // what stored documents use
                        using var shuffled = context.ReadObject(ToDjv(Shuffle(model, rng)), "d", BlittableJsonDocumentBuilder.UsageMode.None);

                        // 1 + 3 + 2
                        Expect(ctx, plain, plain, true, "reflexive", model, crossCheck: !withMetadata, ref pairs);
                        Expect(ctx, plain, shuffled, true, "shuffled insertion order", model, crossCheck: !withMetadata, ref pairs);
                        if (HasNestedContainers(model) == false)
                        {
                            // finding s-5: with nested objects/arrays DocumentCompare falls back to BlittableJsonReaderObject.Equals, which is
                            // representation-sensitive (String vs CompressedString never equal) - excluded here so the fuzz can keep hunting
                            Expect(ctx, plain, compressed, true, "plain vs compressed strings", model, crossCheck: false, ref pairs);
                            Expect(ctx, compressed, shuffled, true, "compressed vs shuffled", model, crossCheck: false, ref pairs);
                        }

                        // 4: a single mutation
                        var mutation = Mutate(model, rng, out var mustDetect, out var what);
                        if (mutation != null)
                        {
                            // same representation as `plain` - representation differences are s-5, tested (and excluded) above
                            using var mutated = context.ReadObject(ToDjv(mutation), "d", BlittableJsonDocumentBuilder.UsageMode.None);
                            Expect(ctx, plain, mutated, !mustDetect, $"mutation: {what}", model, crossCheck: !withMetadata, ref pairs);
                        }
                    }
                    catch (StressFailure)
                    {
                        // already recorded by Expect
                    }
                    catch (Exception e)
                    {
                        ctx.Fail($"exception on document #{docs} (metadata {withMetadata}): {e}" + Environment.NewLine + "DOC: " + Describe(model));
                    }

                    if (ctx.Failures.Count > 0)
                        break;

                    if (docs % 20000 == 0)
                        Console.WriteLine($"  {docs} documents, {pairs} comparisons, clean");
                }
            }

            ctx.Iterations = (int)Math.Min(int.MaxValue, docs);
            Console.WriteLine($"  {docs} documents, {pairs} comparisons");
            return ctx.Finish();
        }

        private sealed class StressFailure : Exception
        {
        }

        private static void Expect(StressContext ctx, BlittableJsonReaderObject a, BlittableJsonReaderObject b, bool expectEqual, string what,
            Dictionary<string, object> model, bool crossCheck, ref long pairs)
        {
            pairs++;
            var forward = DocumentCompare.IsEqualTo(a, b, DocumentCompare.DocumentCompareOptions.Default);
            var backward = DocumentCompare.IsEqualTo(b, a, DocumentCompare.DocumentCompareOptions.Default);
            var fwdEqual = (forward & DocumentCompareResult.Equal) == DocumentCompareResult.Equal;
            var bwdEqual = (backward & DocumentCompareResult.Equal) == DocumentCompareResult.Equal;

            if (fwdEqual != bwdEqual)
            {
                ctx.Fail($"DocumentCompare is not symmetric ({what}): forward {forward}, backward {backward}\nDOC: {Describe(model)}");
                throw new StressFailure();
            }

            if (fwdEqual != expectEqual)
            {
                ctx.Fail($"DocumentCompare.IsEqualTo returned {forward}, expected {(expectEqual ? "Equal" : "NotEqual")} ({what})" + Environment.NewLine + "PER-PROPERTY: " + DiffProperties(a, b) + Environment.NewLine + "DOC: " + Describe(model));
                throw new StressFailure();
            }

            if (crossCheck)
            {
                // second implementation must agree (metadata-free, same-representation pairs only: Equals has its own metadata rules
                // and reads the other side's value with this side's token, so String vs CompressedString never compares equal there)
                var equals = a.Equals(b);
                if (equals != expectEqual)
                {
                    ctx.Fail($"BlittableJsonReaderObject.Equals returned {equals} while DocumentCompare returned {forward} ({what}) - the two implementations disagree\nDOC: {Describe(model)}");
                    throw new StressFailure();
                }
            }
        }

        private static bool HasNestedContainers(Dictionary<string, object> model) =>
            model.Values.Any(v => v is Dictionary<string, object> || v is List<object>);

        // lists properties whose tokens or materialized values differ between a and b (public API only)
        private static string DiffProperties(BlittableJsonReaderObject a, BlittableJsonReaderObject b)
        {
            var lines = new List<string>();
            var pa = new BlittableJsonReaderObject.PropertyDetails();
            var pb = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < a.Count; i++)
            {
                a.GetPropertyByIndex(i, ref pa);
                var j = b.GetPropertyIndex(pa.Name);
                if (j < 0)
                {
                    lines.Add($"'{pa.Name}' missing in b");
                    continue;
                }

                b.GetPropertyByIndex(j, ref pb);
                var same = (pa.Value == null && pb.Value == null) || (pa.Value?.Equals(pb.Value) ?? false);
                if (pa.Token != pb.Token || same == false)
                    lines.Add($"'{pa.Name}': a={pa.Token} ({pa.Value?.GetType().Name}) b={pb.Token} ({pb.Value?.GetType().Name}) valuesEqual={same}");
            }

            return lines.Count == 0 ? "(no per-property difference visible through the public API)" : string.Join("; ", lines.Take(12));
        }

        // ---- model: ordered dictionary of name -> value (string | long | double | bool | null | List<object> | Dictionary<string,object>) ----

        private static List<string> BuildNamePool()
        {
            var pool = new List<string>();
            var bases = new[] { "Name", "name", "NAME", "nAme", "a", "aa", "aaa", "ab", "abc", "b", "Id", "id", "ID", "Value", "value", "Count", "Zeta", "alpha", "Alpha", "żółw", "Żółw", "日本", "emoji😀", "q\"uote", "new\nline", "tab\tkey", "back\\slash", "123", "0", "@custom", "@Custom", "x", "X", "Long", new string('n', 300), new string('n', 299) + "m", "Prefix", "PrefixLonger", "PrefixLongest" };
            pool.AddRange(bases);
            for (var i = 0; i < 40; i++)
                pool.Add("p" + i);
            return pool;
        }

        private static Dictionary<string, object> RandomObject(Random rng, List<string> names, int depth, bool withMetadata)
        {
            var count = depth == 0 ? rng.Next(0, 60) : rng.Next(0, 6);
            var obj = new Dictionary<string, object>();
            for (var i = 0; i < count; i++)
            {
                var name = names[rng.Next(names.Count)];
                if (obj.ContainsKey(name))
                    continue;
                obj[name] = RandomValue(rng, names, depth);
            }

            if (withMetadata && depth == 0)
            {
                var md = new Dictionary<string, object>
                {
                    ["@collection"] = "Coll" + rng.Next(3),
                    ["@id"] = "docs/" + rng.Next(100),
                    ["@change-vector"] = "A:" + rng.Next(1000),
                    ["@last-modified"] = "2026-09-12T00:00:00.000Z",
                    ["@flags"] = rng.Next(2) == 0 ? "HasRevisions" : "HasAttachments"
                };
                if (rng.Next(2) == 0)
                    md["@expires"] = "2027-01-01T00:00:00.000Z";
                if (rng.Next(2) == 0)
                    md["@Custom-Meta"] = rng.Next(5);
                obj["@metadata"] = md;
            }

            return obj;
        }

        private static object RandomValue(Random rng, List<string> names, int depth)
        {
            switch (rng.Next(12))
            {
                case 0: return null;
                case 1: return rng.Next(2) == 0;
                case 2: return (long)rng.Next(-1000, 1000);
                case 3: return rng.NextInt64();
                case 4: return rng.Next(2) == 0 ? 5.0 : rng.NextDouble() * 1000;
                case 5: return "s" + rng.Next(100);
                case 6: return new string((char)('a' + rng.Next(3)), 50 + rng.Next(2000)); // compressible
                case 7: return "żółć-" + rng.Next(10) + "-😀";
                case 8: return depth < 3 ? RandomObject(rng, names, depth + 1, false) : (object)"leaf";
                case 9: return depth < 3 ? RandomArray(rng, names, depth + 1) : (object)"leaf";
                case 10: return "";
                default: return "5.0";
            }
        }

        private static List<object> RandomArray(Random rng, List<string> names, int depth)
        {
            var list = new List<object>();
            var n = rng.Next(0, 5);
            for (var i = 0; i < n; i++)
                list.Add(RandomValue(rng, names, depth));
            return list;
        }

        private static DynamicJsonValue ToDjv(Dictionary<string, object> obj)
        {
            var djv = new DynamicJsonValue();
            foreach (var kv in obj)
                djv[kv.Key] = Convert(kv.Value);
            return djv;
        }

        private static object Convert(object value) => value switch
        {
            Dictionary<string, object> o => ToDjv(o),
            List<object> l => new DynamicJsonArray(l.Select(Convert)),
            _ => value
        };

        private static Dictionary<string, object> Shuffle(Dictionary<string, object> obj, Random rng)
        {
            var keys = obj.Keys.ToList();
            for (var i = keys.Count - 1; i > 0; i--)
            {
                var j = rng.Next(i + 1);
                (keys[i], keys[j]) = (keys[j], keys[i]);
            }

            var result = new Dictionary<string, object>();
            foreach (var k in keys)
            {
                result[k] = obj[k] switch
                {
                    Dictionary<string, object> o => Shuffle(o, rng),
                    List<object> l => l.Select(v => v is Dictionary<string, object> d ? Shuffle(d, rng) : v).ToList(),
                    var v => v
                };
            }

            return result;
        }

        // one change; mustDetect says whether DocumentCompare is required to report NotEqual
        private static Dictionary<string, object> Mutate(Dictionary<string, object> obj, Random rng, out bool mustDetect, out string what)
        {
            var copy = new Dictionary<string, object>(obj);
            var keys = copy.Keys.ToList();
            mustDetect = true;
            what = null;
            if (keys.Count == 0)
                return null;

            var key = keys[rng.Next(keys.Count)];
            if (key == "@metadata")
            {
                var md = new Dictionary<string, object>((Dictionary<string, object>)copy["@metadata"]);
                var mdKeys = md.Keys.ToList();
                var mk = mdKeys[rng.Next(mdKeys.Count)];
                md[mk] = "changed-" + rng.Next();
                copy["@metadata"] = md;
                mustDetect = SignificantMetadata.Contains(mk); // case-sensitive, as the span comparison is
                what = $"@metadata.{mk} changed (significant: {mustDetect})";
                return copy;
            }

            switch (rng.Next(4))
            {
                case 0:
                    copy.Remove(key);
                    what = $"removed '{key}'";
                    break;
                case 1:
                    copy[key] = copy[key] is long l ? l + 1 : (object)("m" + rng.Next());
                    what = $"changed value of '{key}'";
                    break;
                case 2:
                    var renamed = key.ToUpperInvariant() == key ? key.ToLowerInvariant() : key.ToUpperInvariant();
                    if (renamed == key || copy.ContainsKey(renamed))
                        return null;
                    copy.Remove(key);
                    copy[renamed] = obj[key];
                    what = $"case-renamed '{key}' -> '{renamed}'";
                    break;
                default:
                    copy["extra" + rng.Next(1000)] = rng.Next();
                    what = "added a property";
                    break;
            }

            return copy;
        }

        private static string Describe(Dictionary<string, object> model)
        {
            try
            {
                return System.Text.Json.JsonSerializer.Serialize(model, new System.Text.Json.JsonSerializerOptions { MaxDepth = 32 });
            }
            catch
            {
                return $"({model.Count} properties)";
            }
        }
    }
}
