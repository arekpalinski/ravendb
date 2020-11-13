using System;
using System.Buffers;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Sparrow.Server.Json.Sync
{
    internal static class JsonOperationContextSyncExtensions
    {
        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, BlittableJsonReaderObject json)
        {
            syncContext.EnsureNotDisposed();

            using (var writer = new BlittableJsonTextWriter(syncContext.Context, stream))
            {
                writer.WriteObject(json);
            }
        }

        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, DynamicJsonValue json)
        {
            syncContext.EnsureNotDisposed();

            WriteInternal(syncContext, writer, json);
        }

        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, BlittableJsonReaderObject json)
        {
            syncContext.EnsureNotDisposed();

            WriteInternal(syncContext, writer, json);
        }

        private static void WriteInternal(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, object json)
        {
            syncContext.JsonParserState.Reset();
            syncContext.ObjectJsonParser.Reset(json);

            syncContext.ObjectJsonParser.Read();

            WriteObject(syncContext, writer, syncContext.JsonParserState, syncContext.ObjectJsonParser);

            syncContext.ObjectJsonParser.Reset(null);
        }

        private unsafe static void WriteObject(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            syncContext.EnsureNotDisposed();

            if (state.CurrentTokenType != JsonParserToken.StartObject)
                throw new InvalidOperationException("StartObject expected, but got " + state.CurrentTokenType);

            writer.WriteStartObject();
            bool first = true;
            while (true)
            {
                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");
                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                    throw new InvalidOperationException("Property expected, but got " + state.CurrentTokenType);

                if (first == false)
                    writer.WriteComma();
                first = false;

                var lazyStringValue = syncContext.Context.AllocateStringValue(null, state.StringBuffer, state.StringSize);
                writer.WritePropertyName(lazyStringValue);

                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");

                WriteValue(syncContext, writer, state, parser);
            }

            writer.WriteEndObject();
        }

        private unsafe static void WriteValue(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            switch (state.CurrentTokenType)
            {
                case JsonParserToken.Null:
                    writer.WriteNull();
                    break;

                case JsonParserToken.False:
                    writer.WriteBool(false);
                    break;

                case JsonParserToken.True:
                    writer.WriteBool(true);
                    break;

                case JsonParserToken.String:
                    if (state.CompressedSize.HasValue)
                    {
                        var lazyCompressedStringValue = new LazyCompressedStringValue(null, state.StringBuffer, state.StringSize, state.CompressedSize.Value, syncContext.Context);
                        writer.WriteString(lazyCompressedStringValue);
                    }
                    else
                    {
                        writer.WriteString(syncContext.Context.AllocateStringValue(null, state.StringBuffer, state.StringSize));
                    }
                    break;

                case JsonParserToken.Float:
                    writer.WriteDouble(new LazyNumberValue(syncContext.Context.AllocateStringValue(null, state.StringBuffer, state.StringSize)));
                    break;

                case JsonParserToken.Integer:
                    writer.WriteInteger(state.Long);
                    break;

                case JsonParserToken.StartObject:
                    WriteObject(syncContext, writer, state, parser);
                    break;

                case JsonParserToken.StartArray:
                    WriteArray(syncContext, writer, state, parser);
                    break;

                default:
                    throw new ArgumentOutOfRangeException("Could not understand " + state.CurrentTokenType);
            }
        }

        private static void WriteArray(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            syncContext.EnsureNotDisposed();

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                throw new InvalidOperationException("StartArray expected, but got " + state.CurrentTokenType);

            writer.WriteStartArray();
            bool first = true;
            while (true)
            {
                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                if (first == false)
                    writer.WriteComma();
                first = false;

                WriteValue(syncContext, writer, state, parser);
            }

            writer.WriteEndArray();
        }

        public static BlittableJsonReaderObject ReadForDisk(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string documentId)
        {
            return ParseToMemory(syncContext, stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public static BlittableJsonReaderObject ReadForMemory(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string documentId)
        {
            return ParseToMemory(syncContext, stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        public static unsafe BlittableJsonReaderObject ReadForMemory(this JsonOperationContext.SyncJsonOperationContext syncContext, string jsonString, string documentId)
        {
            // todo: maybe use ManagedPinnedBuffer here
            var maxByteSize = Encodings.Utf8.GetMaxByteCount(jsonString.Length);

            fixed (char* val = jsonString)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(maxByteSize);
                try
                {
                    fixed (byte* buf = buffer)
                    {
                        Encodings.Utf8.GetBytes(val, jsonString.Length, buf, maxByteSize);
                        using (var ms = new MemoryStream(buffer))
                        {
                            return ReadForMemory(syncContext, ms, documentId);
                        }
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }

        private static BlittableJsonReaderObject ParseToMemory(JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode, IBlittableDocumentModifier modifier = null)
        {
            using (syncContext.Context.GetMemoryBuffer(out var bytes))
                return ParseToMemory(syncContext, stream, debugTag, mode, bytes, modifier);
        }

        public static BlittableJsonReaderObject ParseToMemory(
            this JsonOperationContext.SyncJsonOperationContext syncContext,
            Stream stream,
            string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode,
            JsonOperationContext.MemoryBuffer bytes,
            IBlittableDocumentModifier modifier = null)
        {
            syncContext.EnsureNotDisposed();

            syncContext.JsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(syncContext.Context, syncContext.JsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(syncContext.Context, mode, debugTag, parser, syncContext.JsonParserState, modifier: modifier))
            {
                syncContext.Context.CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = stream.Read(bytes.Memory.Memory.Span);
                        syncContext.EnsureNotDisposed();
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }
    }
}
