﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Client;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Utils;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Replication.ReplicationBatchItem;
using Raven.Server.Utils;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public unsafe class CountersStorage
    {
        private const int DbIdAsBase64Size = 22;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        
        private static readonly Slice CountersTombstonesSlice;
        private static readonly Slice AllCountersEtagSlice;
        private static readonly Slice CollectionCountersEtagsSlice;
        private static readonly Slice CounterKeysSlice;

        public static readonly string CountersTombstones = "Counters.Tombstones";

        private static readonly TableSchema CountersSchema = new TableSchema
        {
            TableType = (byte)TableType.Counters
        };

        private enum CountersTable
        {
            // Format of this is:
            // lower document id, record separator, lower counter name, record separator, 16 bytes dbid
            CounterKey = 0,
            Name = 1, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            Etag = 2,
            Value = 3,
            ChangeVector = 4,
            Collection = 5,
            TransactionMarker = 6
        }

        static CountersStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
            	Slice.From(ctx, "AllCountersEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
            	Slice.From(ctx, "CollectionCountersEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
            	Slice.From(ctx, "CounterKeys", ByteStringType.Immutable, out CounterKeysSlice);
            	Slice.From(ctx, CountersTombstones, ByteStringType.Immutable, out CountersTombstonesSlice);
            }
            CountersSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CountersTable.CounterKey,
                Count = 1,
                Name = CounterKeysSlice,
                IsGlobal = true,
            });

            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = AllCountersEtagSlice,
                IsGlobal = true
            });

            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = CollectionCountersEtagsSlice
            });
        }

        public CountersStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(CounterKeysSlice);
            
            TombstonesSchema.Create(tx, CountersTombstonesSlice, 16);
        }

        public IEnumerable<ReplicationBatchItem> GetCountersFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, 0))
            {
                yield return CreateReplicationBatchItem(context, result);
            }
        }

        public IEnumerable<CounterDetail> GetCountersFrom(DocumentsOperationContext context, long etag, int skip, int take)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToCounterDetail(context, result.Reader);
            }
        }

        public IEnumerable<CounterDetail> GetCountersFrom(DocumentsOperationContext context, string collection, long etag, int skip, int take)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = EnsureCountersTableCreated(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;
            
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToCounterDetail(context, result.Reader);
            }
        }

        public static CounterDetail TableValueToCounterDetail(JsonOperationContext context, TableValueReader tvr)
        {
            var (doc, name) = ExtractDocIdAndName(context, tvr);

            return new CounterDetail
            {
                DocumentId = doc,
                LazyDocumentId = doc,
                CounterName = name,
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref tvr),
                TotalValue = TableValueToLong((int)CountersTable.Value, ref tvr),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvr),
            };
        }

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndName(JsonOperationContext context, TableValueReader tvr)
        {
            var p = tvr.Read((int)CountersTable.CounterKey, out var size);
            Debug.Assert(size > DbIdAsBase64Size + 2 /* record separators */);
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = ExtractCounterName(context, tvr);
            return (doc, name);
        }

        public static (string DocId, string CounterName) ExtractDocIdAndCounterName(LazyStringValue counterTombstoneId)
        {
            var p = counterTombstoneId.Buffer;
            var size = counterTombstoneId.Size;
                
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = Encoding.UTF8.GetString(p, sizeOfDocId);
            var name = Encoding.UTF8.GetString(p + sizeOfDocId + 1, size - (sizeOfDocId + 2));

            return (doc, name);
        }

        private static ReplicationBatchItem CreateReplicationBatchItem(DocumentsOperationContext context, Table.TableValueHolder result)
        {
            var (doc, name) = ExtractDocIdAndName(context, result.Reader);

            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Counter,
                Id = doc,
                Etag = TableValueToEtag((int)CountersTable.Etag, ref result.Reader),
                Name = name,
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref result.Reader),
                Value = TableValueToLong((int)CountersTable.Value, ref result.Reader),
                Collection = TableValueToId(context, (int)CountersTable.Collection, ref result.Reader),
                TransactionMarker = TableValueToShort((int)CountersTable.TransactionMarker, nameof(ReplicationBatchItem.TransactionMarker), ref result.Reader),
            };
        }

        public enum PutCounterMode
        {
            None,
            Replication,
            Smuggler,
            Etl
        }

        public void PutCounter(DocumentsOperationContext context, string documentId, string collection, string name, string changeVector, long value, PutCounterMode mode)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = EnsureCountersTableCreated(context.Transaction.InnerTransaction, collectionName);

            using (GetCounterKey(context, documentId, name, mode == PutCounterMode.Etl ? context.Environment.Base64Id : changeVector, out var counterKey))
            {
                using (DocumentIdWorker.GetStringPreserveCase(context, name, out Slice nameSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    switch (mode)
                    {
                        case PutCounterMode.Replication:
                        case PutCounterMode.Smuggler:
                            Debug.Assert(changeVector != null);

                            if (table.ReadByKey(counterKey, out var existing))
                            {
                                var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);

                                if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                                    return;
                            }
                            break;
                    }

                    RemoveTombstoneIfExists(context, documentId, name);

                    var etag = _documentsStorage.GenerateNextEtag();

                    switch (mode)
                    {
                        case PutCounterMode.Etl:
                            Debug.Assert(changeVector == null);

                            changeVector = ChangeVectorUtils
                                .TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty)
                                .ChangeVector;
                            break;
                    }

                    using (Slice.From(context.Allocator, changeVector, out var cv))
                    using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                    {
                        tvb.Add(counterKey);
                        tvb.Add(nameSlice);
                        tvb.Add(Bits.SwapBytes(etag));
                        tvb.Add(value);
                        tvb.Add(cv);
                        tvb.Add(collectionSlice);
                        tvb.Add(context.TransactionMarkerOffset);

                        table.Set(tvb);
                    }

                    context.Transaction.AddAfterCommitNotification(new DocumentChange
                    {
                        ChangeVector = changeVector,
                        Id = documentId,
                        CounterName = name,
                        Type = DocumentChangeTypes.Counter
                    });
                }
            }
        }

        private void RemoveTombstoneIfExists(DocumentsOperationContext context, string documentId, string name)
        {
            using (GetCounterPartialKey(context, documentId, name, out var keyPerfix))
            {
                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstonesSlice);

                if (tombstoneTable.ReadByKey(keyPerfix, out var existingTombstone))
                {
                    tombstoneTable.Delete(existingTombstone.Id);
                }
            }
        }

        private readonly HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public Table EnsureCountersTableCreated(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.Counters);

            if (_tableCreated.Add(collection.Name))
                CountersSchema.Create(tx, tableName, 32);

            return tx.OpenTable(CountersSchema, tableName);
        }

        public string IncrementCounter(DocumentsOperationContext context, string documentId, string collection, string name, long value)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = EnsureCountersTableCreated(context.Transaction.InnerTransaction, collectionName);
            
            using (GetCounterKey(context, documentId, name, context.Environment.Base64Id, out var counterKey))
            {
                long prev = 0;
                if (table.ReadByKey(counterKey, out var existing))
                {
                    prev = *(long*)existing.Read((int)CountersTable.Value, out var size);
                    Debug.Assert(size == sizeof(long));
                }

                RemoveTombstoneIfExists(context, documentId, name);

                var etag = _documentsStorage.GenerateNextEtag();
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty);

                using (Slice.From(context.Allocator, result.ChangeVector, out var cv))

                using (DocumentIdWorker.GetStringPreserveCase(context, name, out Slice nameSlice))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(counterKey);
                    tvb.Add(nameSlice);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(prev + value); //inc
                    tvb.Add(cv);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Set(tvb);
                }

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    ChangeVector = result.ChangeVector,
                    Id = documentId,
                    CounterName = name,
                    Type = DocumentChangeTypes.Counter
                });

                return result.ChangeVector;
            }
        }

        public IEnumerable<string> GetCountersForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (GetCounterPartialKey(context, docId, out var key))
            {
                var prev = string.Empty;
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    var current = ExtractCounterName(context, result.Value.Reader);

                    if (prev.Equals(current))
                    {
                        // already seen this one, skip it 
                        continue;
                    }

                    yield return current;

                    prev = current;
                }
            }
        }

        public long? GetCounterValue(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (GetCounterPartialKey(context, docId, counterName, out var key))
            {
                long? value = null;
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    value = value ?? 0;
                    var pCounterDbValue = result.Value.Reader.Read((int)CountersTable.Value, out var size);
                    Debug.Assert(size == sizeof(long));
                    value += *(long*)pCounterDbValue;
                }

                return value;
            }
        }

        public IEnumerable<(string ChangeVector, long Value)> GetCounterValues(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (GetCounterPartialKey(context, docId, counterName, out var keyPerfix))
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(keyPerfix, Slices.Empty, 0))
                {
                    (string, long) val = ExtractDbIdAndValue(result);
                    yield return val;
                }
            }
        }

        private static (string ChangeVector , long Value) ExtractDbIdAndValue((Slice Key, Table.TableValueHolder Value) result)
        {
            var counterKey = result.Value.Reader.Read((int)CountersTable.CounterKey, out var size);
            Debug.Assert(size > DbIdAsBase64Size);
            var pCounterDbValue = result.Value.Reader.Read((int)CountersTable.Value, out size);
            Debug.Assert(size == sizeof(long));
            var changeVector = result.Value.Reader.Read((int)CountersTable.ChangeVector, out size);
            
            return (Encoding.UTF8.GetString(changeVector, size), *(long*)pCounterDbValue);
        }

        private static LazyStringValue ExtractCounterName(JsonOperationContext context, TableValueReader tvr)
        {
            return TableValueToId(context, (int)CountersTable.Name, ref tvr);

        }

        public ByteStringContext.InternalScope GetCounterKey(DocumentsOperationContext context, string documentId, string name, string changeVector, out Slice partialKeySlice)
        {
            Debug.Assert(changeVector.Length >= DbIdAsBase64Size);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out var nameLower, out _))
            using (Slice.From(context.Allocator, changeVector, out var cv))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       + nameLower.Size
                                                       + 1 // record separator
                                                       + DbIdAsBase64Size, // db id
                                                       out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;
                byte* dest = buffer.Ptr + docIdLower.Size + 1;
                nameLower.CopyTo(dest);
                dest[nameLower.Size] = SpecialChars.RecordSeparator;
                cv.CopyTo(cv.Size - DbIdAsBase64Size, dest, nameLower.Size + 1, DbIdAsBase64Size);

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        public ByteStringContext.InternalScope GetCounterPartialKey(DocumentsOperationContext context, string documentId, string name, out Slice partialKeySlice)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out var nameLower, out _))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       + nameLower.Size
                                                       + 1 // record separator
                                                       , out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;

                byte* dest = buffer.Ptr + docIdLower.Size + 1;
                nameLower.CopyTo(dest);
                dest[nameLower.Size] = SpecialChars.RecordSeparator;

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        public ByteStringContext.InternalScope GetCounterPartialKey(DocumentsOperationContext context, string documentId,  out Slice partialKeySlice)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       , out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        public void DeleteCountersForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will called as part of document's delete, so we don't bother creating
            // tombstones (existing tombstones will remain and be cleaned up by the usual
            // tombstone cleaner task
            
            var table = EnsureCountersTableCreated(context.Transaction.InnerTransaction, collection);

            if (table.NumberOfEntries == 0)
                return; 

            using (GetCounterPartialKey(context, documentId, out var keyPerfix))
            {
                table.DeleteByPrimaryKeyPrefix(keyPerfix);
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, string documentId, string collection, string counterName)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            using (GetCounterPartialKey(context, documentId, counterName, out var keyPerfix))
            {
                var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                return DeleteCounter(context, keyPerfix, collection, lastModifiedTicks,
                    // let's avoid creating a tombstone for missing counter if writing locally
                    forceTombstone: false);
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, Slice key, string collection, long lastModifiedTicks, bool forceTombstone)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = EnsureCountersTableCreated(context.Transaction.InnerTransaction, collectionName);

            long deletedEtag = -1;
            if (table.DeleteByPrimaryKeyPrefix(key, tvh =>
                {
                    long etag = *(long*)tvh.Reader.Read((int)CountersTable.Etag, out var size);
                    deletedEtag = Math.Max(Bits.SwapBytes(etag), deletedEtag);
                    Debug.Assert(size == sizeof(long));
                }) == false
                && forceTombstone == false)
                return null;

            if (deletedEtag == -1)
                deletedEtag = _documentsStorage.GenerateNextEtagForReplicatedTombstoneMissingDocument(context);
            var newEtag = _documentsStorage.GenerateNextEtag();
            var newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);

            CreateTombstone(context, key, collection, deletedEtag, lastModifiedTicks, newEtag, newChangeVector);

            return newChangeVector;
        }

        private void CreateTombstone(DocumentsOperationContext context, Slice keySlice, string collectionName, long deletedEtag, long lastModifiedTicks, long newEtag, string newChangeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstonesSlice);
            using (Slice.From(context.Allocator, newChangeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName, out Slice collectionSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(Bits.SwapBytes(deletedEtag)); // etag that was deleted
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((byte)Tombstone.TombstoneType.Counter);
                tvb.Add(collectionSlice);
                tvb.Add((int)DocumentFlags.None);
                tvb.Add(cv.Content.Ptr, cv.Size); // change vector
                tvb.Add(lastModifiedTicks);
                table.Insert(tvb);
            }
        }

        public static void AssertCounters(BlittableJsonReaderObject document, DocumentFlags flags)
        {
            if ((flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray _) == false)
                {
                    Debug.Assert(false, $"Found {DocumentFlags.HasCounters} flag but {Constants.Documents.Metadata.Counters} is missing from metadata.");
                }
            }
            else
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters))
                {
                    Debug.Assert(false, $"Found {Constants.Documents.Metadata.Counters}({counters.Length}) in metadata but {DocumentFlags.HasCounters} flag is missing.");
                }
            }
        }

        public long GetNumberOfCounterEntries(DocumentsOperationContext context)
        {
            var fstIndex = CountersSchema.FixedSizeIndexes[AllCountersEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public void UpdateDocumentCounters(DocumentsOperationContext context, BlittableJsonReaderObject doc, string docId, BlittableJsonReaderObject metadata, List<CounterOperation> countersOperations)
        {
            List<string> updates = null;
            if (metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters))
            {
                foreach (var operation in countersOperations)
                {
                    // we need to check the updates to avoid inserting duplicate counter names
                    int loc = updates?.BinarySearch(operation.CounterName, StringComparer.OrdinalIgnoreCase) ??
                              counters.BinarySearch(operation.CounterName, StringComparison.OrdinalIgnoreCase);

                    switch (operation.Type)
                    {
                        case CounterOperationType.Increment:
                        case CounterOperationType.Put:
                            if (loc < 0)
                            {
                                CreateUpdatesIfNeeded();
                                updates.Insert(~loc, operation.CounterName);
                            }

                            break;
                        case CounterOperationType.Delete:
                            if (loc >= 0)
                            {
                                CreateUpdatesIfNeeded();
                                updates.RemoveAt(loc);
                            }
                            break;
                        case CounterOperationType.None:
                        case CounterOperationType.Get:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"Unknown value {operation.Type}");
                    }
                }
            }
            else
            {
                updates = new List<string>(countersOperations.Count);
                foreach (var operation in countersOperations)
                {
                    updates.Add(operation.CounterName);
                }
                updates.Sort(StringComparer.OrdinalIgnoreCase);
            }

            if (updates != null)
            {
                if (updates.Count == 0)
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(updates)
                    };
                }

                doc.Modifications = new DynamicJsonValue(doc)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };
            }

            if (metadata.Modifications != null)
            {
                var data = context.ReadObject(doc, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                var flags = data.TryGet(Constants.Documents.Metadata.Key, out metadata) &&
                            metadata.TryGet(Constants.Documents.Metadata.Counters, out object _)
                    ? DocumentFlags.HasCounters
                    : DocumentFlags.None;

                _documentDatabase.DocumentsStorage.Put(context, docId, null, data, flags: flags, nonPersistentFlags: NonPersistentDocumentFlags.ByCountersUpdate);
            }

            void CreateUpdatesIfNeeded()
            {
                if (updates != null)
                    return;

                updates = new List<string>(counters.Length + countersOperations.Count);
                for (int i = 0; i < counters.Length; i++)
                {
                    var val = counters.GetStringByIndex(i);
                    if (val == null)
                        continue;
                    updates.Add(val);
                }
            }
        }

    }
}
