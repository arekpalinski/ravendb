using System;
using System.Collections.Generic;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.ETL;

public unsafe class EtlErrorsStorage(string databaseName)
{
    private const int ErrorsLimitPerEtl = 500;

    private readonly string _processErrorsTableName = GetProcessErrorsTableName(databaseName);
    private readonly string _itemErrorsTableName = GetItemErrorsTableName(databaseName);
    
    private readonly Logger _logger = LoggingSource.Instance.GetLogger<NotificationsStorage>(databaseName);

    protected StorageEnvironment Environment;
    private TransactionContextPool _contextPool;
    
    public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
    {
        Environment = environment;
        _contextPool = contextPool;

        using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            CreateSchema(tx.InnerTransaction);
        }
    }
    
    private void CreateSchema(Transaction tx)
    {
        Schemas.EtlProcessErrors.Current.Create(tx, _processErrorsTableName, 16);
        Schemas.EtlItemErrors.Current.Create(tx, _itemErrorsTableName, 16);
        
        tx.Commit();
    }
    
    internal void StoreProcessError(EtlProcessError processError)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
                
                var createdAtTicks = Bits.SwapBytes(processError.CreatedAt.Ticks);
                var affectedDocumentsCountSwapped = Bits.SwapBytes(processError.AffectedDocumentsCount);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)processError.Step);
                var severitySwapped = Bits.SwapBytes((long)processError.Severity);
                
                var id = context.GetLazyString(processError.Id);
                var etlProcessName = context.GetLazyString(processError.EtlProcessName);
                var error = context.GetLazyString(processError.Error);
                
                using (Slice.From(tx.InnerTransaction.Allocator, etlProcessName, out Slice etlProcessNameSlice))
                {
                    if (table.GetCountOfMatchesFor(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], etlProcessNameSlice) >= ErrorsLimitPerEtl)
                    {
                        DeleteOldestProcessErrorOfTask(processError.EtlProcessName);
                    }
                }
                
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add(etlProcessName.Buffer, etlProcessName.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add((byte*)&affectedDocumentsCountSwapped, sizeof(long));
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));
                    tvb.Add((byte*)&severitySwapped, sizeof(long));
                    tvb.Add(error.Buffer, error.Size);

                    table.Set(tvb);
                }
                
                tx.Commit();
            }
        }
    }
    
    // todo collect all item errors of batch, then store and remove oldest errors over the limit at the end of batch processing in a single transaction
    internal void StoreItemError(EtlItemError itemError)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);

                var createdAtTicks = Bits.SwapBytes(itemError.CreatedAt.Ticks);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)itemError.Step);
                var severitySwapped = Bits.SwapBytes((long)itemError.Severity);

                var id = context.GetLazyString(itemError.Id);
                var etlProcessName = context.GetLazyString(itemError.EtlProcessName);
                var documentId = context.GetLazyString(itemError.DocumentId);

                using (Slice.From(tx.InnerTransaction.Allocator, etlProcessName, out Slice etlProcessNameSlice))
                {
                    if (table.GetCountOfMatchesFor(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], etlProcessNameSlice) >= ErrorsLimitPerEtl)
                    {
                        DeleteOldestItemErrorOfTask(itemError.EtlProcessName);
                    }
                }

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add(etlProcessName.Buffer, etlProcessName.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add(documentId.Buffer, documentId.Size);
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));
                    tvb.Add((byte*)&severitySwapped, sizeof(long));

                    table.Set(tvb);
                }
                
                tx.Commit();
            }
        }
    }
    
    public IDisposable ReadProcessError(string id, out EtlProcessErrorTableValue value)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;

            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            value = GetProcessError(id, tx);
            
            return scope.Delay();
        }
    }
    
    public IDisposable ReadItemError(string id, out EtlItemErrorTableValue value)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;

            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            value = GetItemError(id, tx);
            
            return scope.Delay();
        }
    }
    
    private EtlProcessErrorTableValue GetProcessError(string id, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
        if (table == null)
            return null;

        using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            return ReadProcessError(ref tvr);
        }
    }
    
    private EtlItemErrorTableValue GetItemError(string id, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
        if (table == null)
            return null;

        using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            return ReadItemError(ref tvr);
        }
    }
    
    private EtlProcessErrorTableValue ReadProcessError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.CreatedAtIndex, out _)));
        var etlProcessName = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.EtlProcessNameIndex);
        var affectedDocumentsCount = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.AffectedDocumentsCountIndex, out _));
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.StepIndex, out _));
        var severity = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.SeverityIndex, out _));
        var message = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.MessageIndex);
        
        return new EtlProcessErrorTableValue
        {
            CreatedAt = createdAt,
            EtlProcessName = etlProcessName,
            AffectedDocumentsCount = affectedDocumentsCount,
            Step = step,
            Severity = severity,
            Error = message
        };
    }
    
    private EtlItemErrorTableValue ReadItemError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.CreatedAtIndex, out _)));
        var etlProcessName = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.EtlProcessNameIndex);
        var documentId = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.DocumentIdIndex);
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.StepIndex, out _));
        var severity = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.SeverityIndex, out _));
        
        return new EtlItemErrorTableValue
        {
            CreatedAt = createdAt,
            EtlProcessName = etlProcessName,
            DocumentId = documentId,
            Severity = severity,
            Step = step
        };
    }
    
    private IEnumerable<EtlProcessErrorTableValue> ReadProcessErrorsByCreatedAtIndex(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
        if (table == null)
            yield break;

        foreach (var tvr in table.SeekForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return ReadProcessError(ref tvr.Result.Reader);
        }
    }
    
    private IEnumerable<EtlItemErrorTableValue> ReadItemErrorsByCreatedAtIndex(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
        if (table == null)
            yield break;

        foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return ReadItemError(ref tvr.Result.Reader);
        }
    }
    
    public IDisposable ReadProcessErrorsOrderedByCreationDate(out IEnumerable<EtlProcessErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadProcessErrorsByCreatedAtIndex(context);

            return scope.Delay();
        }
    }
    
    public IDisposable ReadItemErrorsOrderedByCreationDate(out IEnumerable<EtlItemErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadItemErrorsByCreatedAtIndex(context);

            return scope.Delay();
        }
    }

    public IDisposable ReadProcessErrorsOfTask(string etlTaskName, out IEnumerable<EtlProcessErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadProcessErrorsOfTask(context, etlTaskName);

            return scope.Delay();
        }
    }
    
    public IDisposable ReadItemErrorsOfTask(string etlTaskName, out IEnumerable<EtlItemErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadItemErrorsOfTask(context, etlTaskName);

            return scope.Delay();
        }
    }
    
    private IEnumerable<EtlProcessErrorTableValue> ReadProcessErrorsOfTask(TransactionOperationContext context, string etlTaskName)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
        if (table == null)
            yield break;

        using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
        {
            foreach (var tvr in table.SeekForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], taskNameSlice, 0))
            {
                var error = ReadProcessError(ref tvr.Result.Reader);

                if (error.EtlProcessName != etlTaskName)
                    yield break;

                yield return error;
            }
        }
    }
    
    private IEnumerable<EtlItemErrorTableValue> ReadItemErrorsOfTask(TransactionOperationContext context, string etlTaskName)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
        if (table == null)
            yield break;

        using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
        {
            foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], taskNameSlice, 0))
            {
                var error = ReadItemError(ref tvr.Result.Reader);

                if (error.EtlProcessName != etlTaskName)
                    yield break;

                yield return error;
            }
        }
    }
    
    public void DeleteProcessErrorsOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
            if (table == null)
                return;

            var idsToDelete = new List<string>();
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], taskNameSlice, 0))
                {
                    var error = ReadProcessError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != etlTaskName)
                        break;

                    idsToDelete.Add(error.Id);
                }
            }

            foreach (var id in idsToDelete)
            {
                using (Slice.From(context.Transaction.InnerTransaction.Allocator, id, out Slice errorId))
                {
                    table.DeleteByKey(errorId);
                }
            }
            
            context.Transaction.Commit();
        }
    }

    public void DeleteItemErrorsOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
            if (table == null)
                return;

            var idsToDelete = new List<string>();
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], taskNameSlice, 0))
                {
                    var error = ReadItemError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != etlTaskName)
                        break;

                    idsToDelete.Add(error.Id);
                }
            }

            foreach (var id in idsToDelete)
            {
                using (Slice.From(context.Transaction.InnerTransaction.Allocator, id, out Slice errorId))
                {
                    table.DeleteByKey(errorId);
                }
            }
            
            context.Transaction.Commit();
        }
    }

    private void DeleteOldestProcessErrorOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
            if (table == null)
                return;
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], taskNameSlice, 0))
                {
                    var error = ReadProcessError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != etlTaskName)
                        break;

                    using (Slice.From(context.Transaction.InnerTransaction.Allocator, error.Id, out Slice errorId))
                    {
                        table.DeleteByKey(errorId);
                        context.Transaction.Commit();
                        return;
                    }
                }
            }
        }
    }

    private void DeleteOldestItemErrorOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
            if (table == null)
                return;
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], taskNameSlice, 0))
                {
                    var error = ReadItemError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != etlTaskName)
                        break;

                    using (Slice.From(context.Transaction.InnerTransaction.Allocator, error.Id, out Slice errorId))
                    {
                        table.DeleteByKey(errorId);
                        context.Transaction.Commit();
                        return;
                    }
                }
            }
        }
    }
    
    private void CleanupProcessErrors(Table table)
    {
        if (table.NumberOfEntries <= ErrorsLimitPerEtl)
            return;

        var numberOfEntriesToDelete = table.NumberOfEntries - ErrorsLimitPerEtl;
        table.DeleteForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByCreatedAt], Slices.BeforeAllKeys, false, numberOfEntriesToDelete);
    }
    
    private void CleanupItemErrors(Table table)
    {
        if (table.NumberOfEntries <= ErrorsLimitPerEtl)
            return;

        var numberOfEntriesToDelete = table.NumberOfEntries - ErrorsLimitPerEtl;
        table.DeleteForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByCreatedAt], Slices.BeforeAllKeys, false, numberOfEntriesToDelete);
    }

    private static string GetProcessErrorsTableName(string databaseName)
    {
        return $"{Schemas.EtlProcessErrors.EtlProcessErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
    
    private static string GetItemErrorsTableName(string databaseName)
    {
        return $"{Schemas.EtlItemErrors.EtlItemErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
}
