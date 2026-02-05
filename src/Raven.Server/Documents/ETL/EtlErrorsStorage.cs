using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.TransactionMerger;
using Sparrow.Binary;
using Sparrow.Json;
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
    private ClusterTransactionOperationsMerger _txMerger;
    
    private List<EtlItemError> _itemErrors;
    
    public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool, ClusterTransactionOperationsMerger txMerger)
    {
        Environment = environment;
        _contextPool = contextPool;
        _txMerger = txMerger;
        
        _itemErrors = new List<EtlItemError>();
        
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

    internal void EnqueueProcessError(EtlProcessError processError)
    {
        _txMerger.EnqueueSync(new StoreEtlProcessErrorCommand(processError, _processErrorsTableName));
    }
    
    internal static void StoreProcessError<T>(TransactionOperationContext<T> context, EtlProcessError processError, string tableName)
        where T : RavenTransaction
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);
                
        var createdAtTicks = Bits.SwapBytes(processError.CreatedAt.Ticks);
        var affectedDocumentsCountSwapped = Bits.SwapBytes(processError.AffectedDocumentsCount);
        var stepSwapped = Bits.SwapBytes((long)processError.Step);
        var severitySwapped = Bits.SwapBytes((long)processError.Severity);
                
        var id = context.GetLazyString(processError.Id);
        var etlProcessName = context.GetLazyString(processError.EtlProcessName);
        var error = context.GetLazyString(processError.Error);
        
        using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlProcessName, out Slice etlProcessNameSlice))
        {
            if (table.GetCountOfMatchesFor(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], etlProcessNameSlice) >= ErrorsLimitPerEtl)
            {
                DeleteOldestProcessErrorOfTask(table, context, processError.EtlProcessName);
            }
        }
                
        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(id.Buffer, id.Size);
            tvb.Add(etlProcessName.Buffer, etlProcessName.Size);
            tvb.Add((byte*)&createdAtTicks, sizeof(long));
            tvb.Add((byte*)&affectedDocumentsCountSwapped, sizeof(long));
            tvb.Add((byte*)&stepSwapped, sizeof(long));
            tvb.Add((byte*)&severitySwapped, sizeof(long));
            tvb.Add(error.Buffer, error.Size);

            table.Set(tvb);
        }
    }

    internal void RecordItemError(EtlItemError itemError)
    {
        _itemErrors.Add(itemError);
    }

    internal void StoreItemErrors(string processName)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);

                foreach (var itemError in _itemErrors)
                {
                    StoreItemError(itemError, table, context);
                }
                
                DeleteOldestItemErrorsOfProcess(processName, table, context);
                
                tx.Commit();
            }
        }
        
        _itemErrors.Clear();
    }
    
    private static void StoreItemError(EtlItemError itemError, Table table, JsonOperationContext context)
    {
        var createdAtTicks = Bits.SwapBytes(itemError.CreatedAt.Ticks);
        var stepSwapped = Bits.SwapBytes((long)itemError.Step);
        var severitySwapped = Bits.SwapBytes((long)itemError.Severity);

        var id = context.GetLazyString(itemError.Id);
        var etlProcessName = context.GetLazyString(itemError.EtlProcessName);
        var documentId = context.GetLazyString(itemError.DocumentId);
        var error = context.GetLazyString(itemError.Error);

        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(id.Buffer, id.Size);
            tvb.Add(etlProcessName.Buffer, etlProcessName.Size);
            tvb.Add((byte*)&createdAtTicks, sizeof(long));
            tvb.Add(documentId.Buffer, documentId.Size);
            tvb.Add((byte*)&stepSwapped, sizeof(long));
            tvb.Add((byte*)&severitySwapped, sizeof(long));
            tvb.Add(error.Buffer, error.Size);

            table.Set(tvb);
        }
    }
    
    private static EtlProcessErrorTableValue ReadProcessError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.CreatedAtIndex, out _)));
        var etlProcessName = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.EtlProcessNameIndex);
        var affectedDocumentsCount = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.AffectedDocumentsCountIndex, out _));
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.StepIndex, out _));
        var severity = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.SeverityIndex, out _));
        var error = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.ErrorIndex);
        
        return new EtlProcessErrorTableValue
        {
            CreatedAt = createdAt,
            EtlProcessName = etlProcessName,
            AffectedDocumentsCount = affectedDocumentsCount,
            Step = step,
            Severity = severity,
            Error = error
        };
    }
    
    private EtlItemErrorTableValue ReadItemError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.CreatedAtIndex, out _)));
        var etlProcessName = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.EtlProcessNameIndex);
        var documentId = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.DocumentIdIndex);
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.StepIndex, out _));
        var severity = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.SeverityIndex, out _));
        var error = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.ErrorIndex);
        
        return new EtlItemErrorTableValue
        {
            CreatedAt = createdAt,
            EtlProcessName = etlProcessName,
            DocumentId = documentId,
            Severity = severity,
            Step = step,
            Error = error
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

    public void DeleteErrorsOfProcess(string processName)
    {
        DeleteProcessErrorsOfProcess(processName);
        DeleteItemErrorsOfProcess(processName);
    }
    
    private void DeleteProcessErrorsOfProcess(string processName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, _processErrorsTableName);
            if (table == null)
                return;

            var idsToDelete = new List<string>();
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, processName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], taskNameSlice, 0))
                {
                    var error = ReadProcessError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != processName)
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

    private void DeleteItemErrorsOfProcess(string processName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
            if (table == null)
                return;

            var idsToDelete = new List<string>();
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, processName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], taskNameSlice, 0))
                {
                    var error = ReadItemError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != processName)
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

    private static void DeleteOldestProcessErrorOfTask<T>(Table table, TransactionOperationContext<T> context, string etlTaskName)
        where T : RavenTransaction
    {
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

    private void DeleteOldestItemErrorsOfProcess(string processName, Table table, TransactionOperationContext context)
    {
        var idsToDelete = new List<string>();
            
        using (Slice.From(context.Transaction.InnerTransaction.Allocator, processName, out Slice processNameSlice))
        {
            var numberOfDeletedErrors = 0;
            var numberOfErrorsToDelete = table.GetCountOfMatchesFor(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], processNameSlice) - ErrorsLimitPerEtl;
                
            if (numberOfErrorsToDelete <= 0)
                return;
                
            foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], processNameSlice, 0))
            {
                var error = ReadItemError(ref tvr.Result.Reader);

                if (error.EtlProcessName != processName || numberOfDeletedErrors == numberOfErrorsToDelete)
                    break;

                idsToDelete.Add(error.Id);
                numberOfDeletedErrors++;
            }
        }

        foreach (var id in idsToDelete)
        {
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, id, out Slice errorId))
            {
                table.DeleteByKey(errorId);
            }
        }
    }

    internal void DeleteOldestItemErrorsOfTask(string etlProcessName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, _itemErrorsTableName);
            if (table == null)
                return;
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlProcessName, out Slice processNameSlice))
            {
                var numberOfErrorsToDelete = table.GetCountOfMatchesFor(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByEtlProcessName], processNameSlice) - ErrorsLimitPerEtl;
                
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], processNameSlice, 0))
                {
                    var error = ReadItemError(ref tvr.Result.Reader);

                    if (error.EtlProcessName != etlProcessName)
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

    private static string GetProcessErrorsTableName(string databaseName)
    {
        return $"{Schemas.EtlProcessErrors.EtlProcessErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
    
    private static string GetItemErrorsTableName(string databaseName)
    {
        return $"{Schemas.EtlItemErrors.EtlItemErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
}
