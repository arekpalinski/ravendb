using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public static class BlittableJsonTextWriterExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask WriteArrayAsync<T>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string name, IEnumerable<T> items,
            Func<AsyncBlittableJsonTextWriter, JsonOperationContext, T, ValueTask> onWrite, CancellationToken token = default)
        {
            await writer.WritePropertyNameAsync(name, token).ConfigureAwait(false);

            await writer.WriteStartArrayAsync(token).ConfigureAwait(false);
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    await writer.WriteCommaAsync(token).ConfigureAwait(false);

                first = false;

                await onWrite(writer, context, item).ConfigureAwait(false);
            }

            await writer.WriteEndArrayAsync(token).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, Memory<double> items, CancellationToken token = default)
        {
            await writer.WritePropertyNameAsync(name, token).ConfigureAwait(false);

            await writer.WriteStartArrayAsync(token).ConfigureAwait(false);
            for (int i = 0; i < items.Length; i++)
            {
                if (i > 0)
                    await writer.WriteCommaAsync(token).ConfigureAwait(false);
                await writer.WriteDoubleAsync(items.Span[i], token).ConfigureAwait(false);
            }
            await writer.WriteEndArrayAsync(token).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IEnumerable<LazyStringValue> items, CancellationToken token = default)
        {
            await writer.WritePropertyNameAsync(name, token).ConfigureAwait(false);

            await writer.WriteStartArrayAsync(token).ConfigureAwait(false);
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    await writer.WriteCommaAsync(token).ConfigureAwait(false);
                first = false;

                await writer.WriteStringAsync(item, token: token).ConfigureAwait(false);
            }
            await writer.WriteEndArrayAsync(token).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IEnumerable<string> items, CancellationToken token = default)
        {
            await writer.WritePropertyNameAsync(name, token).ConfigureAwait(false);

            if (items == null)
            {
                await writer.WriteNullAsync(token).ConfigureAwait(false);
                return;
            }

            await writer.WriteStartArrayAsync(token).ConfigureAwait(false);
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    await writer.WriteCommaAsync(token).ConfigureAwait(false);
                first = false;

                await writer.WriteStringAsync(item, token: token).ConfigureAwait(false);
            }
            await writer.WriteEndArrayAsync(token).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IEnumerable<DynamicJsonValue> items, JsonOperationContext context)
        {
            await writer.WritePropertyNameAsync(name).ConfigureAwait(false);

            await writer.WriteStartArrayAsync().ConfigureAwait(false);
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    await writer.WriteCommaAsync().ConfigureAwait(false);
                first = false;

                await context.WriteAsync(writer, item).ConfigureAwait(false);
            }
            await writer.WriteEndArrayAsync().ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async ValueTask WriteArrayAsync(this AsyncBlittableJsonTextWriter writer, string name, IEnumerable<BlittableJsonReaderObject> items)
        {
            await writer.WritePropertyNameAsync(name).ConfigureAwait(false);

            await writer.WriteStartArrayAsync().ConfigureAwait(false);
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    await writer.WriteCommaAsync().ConfigureAwait(false);
                first = false;

                await writer.WriteObjectAsync(item).ConfigureAwait(false);
            }
            await writer.WriteEndArrayAsync().ConfigureAwait(false);
        }
    }
}
