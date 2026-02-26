using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.ETL;

public unsafe class EtlErrorsStorage
{
    private const int ErrorsLimitPerEtl = 500;

    private StorageEnvironment _environment;
    private DocumentsContextPool _contextPool;
    private DocumentsTransactionOperationsMerger _txMerger;
    private EtlLoader _etlLoader;
    
    public void Initialize(StorageEnvironment environment, DocumentsContextPool contextPool, DocumentsTransactionOperationsMerger txMerger, EtlLoader etlLoader)
    {
        _environment = environment;
        _contextPool = contextPool;
        _txMerger = txMerger;
        _etlLoader = etlLoader;
    }

    internal void CreateEtlErrorsTablesForProcess(string processName)
    {
        var processErrorsTableName = GetProcessErrorsTableName(processName);
        var itemErrorsTableName = GetItemErrorsTableName(processName);
        
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = _environment.WriteTransaction(context.PersistentContext))
        {
            Schemas.EtlProcessErrors.Current.Create(tx, processErrorsTableName, 16);
            Schemas.EtlItemErrors.Current.Create(tx, itemErrorsTableName, 16);
            
            tx.Commit();
        }
    }

    internal void DeleteEtlErrorsTablesForProcess(string processName)
    {
        var processErrorsTableName = GetProcessErrorsTableName(processName);
        var itemErrorsTableName = GetItemErrorsTableName(processName);
        
        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = _environment.WriteTransaction(context.PersistentContext))
        {
            tx.DeleteTable(processErrorsTableName);
            tx.DeleteTable(itemErrorsTableName);
            
            tx.Commit();
        }
    }

    internal void EnqueueProcessError(EtlProcessError processError)
    {
        var tableName = GetProcessErrorsTableName(processError.EtlProcessName);
        
        _txMerger.EnqueueSync(new StoreEtlProcessErrorCommand(processError, tableName));
    }
    
    internal static void StoreProcessError<T>(TransactionOperationContext<T> context, EtlProcessError processError, string tableName)
        where T : RavenTransaction
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);
                            
        var createdAtTicks = Bits.SwapBytes(processError.CreatedAt.Ticks);
        var affectedDocumentsCountSwapped = Bits.SwapBytes(processError.AffectedDocumentsCount);
        var stepSwapped = Bits.SwapBytes((long)processError.Step);
                            
        var id = context.GetLazyString(processError.Id);
        var etlProcessName = context.GetLazyString(processError.EtlProcessName);
        var error = context.GetLazyString(processError.Error);
        var additionalInfo = context.GetLazyString(processError.AdditionalInfo);
                    
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
            tvb.Add(error.Buffer, error.Size);
            tvb.Add(additionalInfo.Buffer, additionalInfo.Size);
            
            table.Set(tvb);
        }
    }

    internal void EnqueueItemErrors(string processName, List<EtlItemError> itemErrors)
    {
        var tableName = GetItemErrorsTableName(processName);
        
        _txMerger.EnqueueSync(new StoreEtlItemErrorsCommand(itemErrors, tableName));
    }
    
    internal static void StoreItemErrors<T>(TransactionOperationContext<T> context, List<EtlItemError> itemErrors, string tableName)
        where T : RavenTransaction
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, tableName);

        foreach (var itemError in itemErrors)
        {
            StoreItemError(itemError, table, context);
        }
                
        DeleteOldestItemErrorsOfEtl(table);
    }
    
    private static void StoreItemError(EtlItemError itemError, Table table, JsonOperationContext context)
    {
        var createdAtTicks = Bits.SwapBytes(itemError.CreatedAt.Ticks);
        var stepSwapped = Bits.SwapBytes((long)itemError.Step);

        var id = context.GetLazyString(itemError.Id);
        var etlProcessName = context.GetLazyString(itemError.EtlProcessName);
        var documentId = context.GetLazyString(itemError.DocumentId);
        var error = context.GetLazyString(itemError.Error);
        var additionalInfo = context.GetLazyString(itemError.AdditionalInfo);

        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(id.Buffer, id.Size);
            tvb.Add(etlProcessName.Buffer, etlProcessName.Size);
            tvb.Add((byte*)&createdAtTicks, sizeof(long));
            tvb.Add(documentId.Buffer, documentId.Size);
            tvb.Add((byte*)&stepSwapped, sizeof(long));
            tvb.Add(error.Buffer, error.Size);
            tvb.Add(additionalInfo.Buffer, additionalInfo.Size);

            table.Set(tvb);
        }
    }
    
    private static EtlProcessErrorTableValue ReadProcessError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.CreatedAtIndex, out _)));
        var etlProcessName = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.EtlProcessNameIndex);
        var affectedDocumentsCount = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.AffectedDocumentsCountIndex, out _));
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlProcessErrors.EtlProcessErrorsTable.StepIndex, out _));
        var error = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.ErrorIndex);
        var additionalInfo = reader.ReadString(Schemas.EtlProcessErrors.EtlProcessErrorsTable.AdditionalInfoIndex);
        
        return new EtlProcessErrorTableValue
        {
            CreatedAt = createdAt,
            EtlProcessName = etlProcessName,
            AffectedDocumentsCount = affectedDocumentsCount,
            Step = step,
            Error = error,
            AdditionalInfo = additionalInfo
        };
    }
    
    private static EtlItemErrorTableValue ReadItemError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.CreatedAtIndex, out _)));
        var etlProcessName = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.EtlProcessNameIndex);
        var documentId = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.DocumentIdIndex);
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlItemErrors.EtlItemErrorsTable.StepIndex, out _));
        var error = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.ErrorIndex);
        var additionalInfo = reader.ReadString(Schemas.EtlItemErrors.EtlItemErrorsTable.AdditionalInfoIndex);
        
        return new EtlItemErrorTableValue
        {
            CreatedAt = createdAt,
            EtlProcessName = etlProcessName,
            DocumentId = documentId,
            Step = step,
            Error = error,
            AdditionalInfo = additionalInfo
        };
    }
    
    /*
    private static IEnumerable<EtlProcessErrorTableValue> ReadProcessErrorsByCreatedAtIndex(string etlProcessName, DocumentsOperationContext context)
    {
        var tableName = GetProcessErrorsTableName(etlProcessName);
        
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);
        if (table == null)
            yield break;

        foreach (var tvr in table.SeekForwardFrom(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return ReadProcessError(ref tvr.Result.Reader);
        }
    }
    
    private static IEnumerable<EtlItemErrorTableValue> ReadItemErrorsByCreatedAtIndex(string etlProcessName, DocumentsOperationContext context)
    {
        var tableName = GetProcessErrorsTableName(etlProcessName);
        
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, tableName);
        if (table == null)
            yield break;

        foreach (var tvr in table.SeekForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return ReadItemError(ref tvr.Result.Reader);
        }
    }
    
    public IDisposable ReadProcessErrorsOrderedByCreationDate(string etlProcessName, out IEnumerable<EtlProcessErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadProcessErrorsByCreatedAtIndex(etlProcessName, context);

            return scope.Delay();
        }
    }
    
    public IDisposable ReadItemErrorsOrderedByCreationDate(string etlProcessName, out IEnumerable<EtlItemErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadItemErrorsByCreatedAtIndex(etlProcessName, context);

            return scope.Delay();
        }
    }
    */
    
    public IDisposable ReadAllProcessErrors(out List<EtlProcessErrorTableValue> errors)
    {
        errors = [];
        
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            foreach (var process in _etlLoader.Processes)
            {
                var processErrors = ReadProcessErrorsOfEtl(process.Name, context);
                errors.AddRange(processErrors);
            }

            return scope.Delay();
        }
    }
    
    public IDisposable ReadAllItemErrors(out List<EtlItemErrorTableValue> errors)
    {
        errors = [];
        
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            foreach (var process in _etlLoader.Processes)
            {
                var itemErrors = ReadItemErrorsOfEtl(process.Name, context);
                errors.AddRange(itemErrors);
            }

            return scope.Delay();
        }
    }
    
    public long ReadErrorsCount()
    {
        var errorsCount = 0L;

        foreach (var etlProcess in _etlLoader.Processes)
        {
            errorsCount += ReadErrorsCountOfEtl(etlProcess.Name);
        }

        return errorsCount;
    }

    public long ReadErrorsCountOfEtl(string etlProcessName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());
            
            var processErrorsCount = ReadProcessErrorsCountOfEtl(etlProcessName, context);
            var itemErrorsCount = ReadItemErrorsCountOfEtl(etlProcessName, context);

            return processErrorsCount + itemErrorsCount;
        }
    }

    private static long ReadProcessErrorsCountOfEtl(string etlProcessName, DocumentsOperationContext context)
    {
        var tableName = GetProcessErrorsTableName(etlProcessName);
                    
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);
        if (table == null)
            return 0;

        return table.NumberOfEntries;
    }
    
    private static long ReadItemErrorsCountOfEtl(string etlProcessName, DocumentsOperationContext context)
    {
        var tableName = GetItemErrorsTableName(etlProcessName);
                    
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlItemErrors.Current, tableName);
        if (table == null)
            return 0;

        return table.NumberOfEntries;
    }

    public IDisposable ReadProcessErrorsOfEtl(string etlProcessName, out IEnumerable<EtlProcessErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadProcessErrorsOfEtl(etlProcessName, context);

            return scope.Delay();
        }
    }
    
    public IDisposable ReadItemErrorsOfEtl(string etlProcessName, out IEnumerable<EtlItemErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out DocumentsOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadItemErrorsOfEtl(etlProcessName, context);

            return scope.Delay();
        }
    }
    
    private static IEnumerable<EtlProcessErrorTableValue> ReadProcessErrorsOfEtl(string etlProcessName, DocumentsOperationContext context)
    {
        var tableName = GetProcessErrorsTableName(etlProcessName);
        
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);
        if (table == null)
            yield break;

        foreach (var tvh in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
        {
            var error = ReadProcessError(ref tvh.Reader);

            yield return error;
        }
    }
    
    private static IEnumerable<EtlItemErrorTableValue> ReadItemErrorsOfEtl(string etlProcessName, DocumentsOperationContext context)
    {
        var tableName = GetItemErrorsTableName(etlProcessName);
        
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);
        if (table == null)
            yield break;

        foreach (var tvh in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
        {
            var error = ReadItemError(ref tvh.Reader);

            yield return error;
        }
    }

    internal EtlProcessErrorTableValue ReadLatestProcessErrorOfEtl(string etlProcessName)
    {
        var tableName = GetProcessErrorsTableName(etlProcessName);

        using (_contextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlProcessErrors.Current, tableName);

            if (table == null)
                return null;

            var tvh = table.SeekOneForwardFromPrefix(Schemas.EtlProcessErrors.Current.Indexes[Schemas.EtlProcessErrors.ByCreatedAt], Slices.BeforeAllKeys);
            
            if (tvh == null)
                return null;
            
            return ReadProcessError(ref tvh.Reader);
        }
    }

    public void DeleteAllEtlErrors()
    {
        foreach (var etlProcess in _etlLoader.Processes)
        {
            DeleteErrorsOfEtl(etlProcess.Name);
        }
    }
    
    public void DeleteErrorsOfEtl(string etlProcessName)
    {
        DeleteEtlErrorsTablesForProcess(etlProcessName);
        CreateEtlErrorsTablesForProcess(etlProcessName);
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

    private static void DeleteOldestItemErrorsOfEtl(Table table)
    {
        if (table == null || table.NumberOfEntries <= ErrorsLimitPerEtl)
            return;

        var numberOfEntriesToDelete = table.NumberOfEntries - ErrorsLimitPerEtl;
        table.DeleteForwardFrom(Schemas.EtlItemErrors.Current.Indexes[Schemas.EtlItemErrors.ByEtlProcessName], Slices.BeforeAllKeys, false, numberOfEntriesToDelete);
    }
    
    private static string GetProcessErrorsTableName(string etlProcessName)
    {
        return $"{Schemas.EtlProcessErrors.EtlProcessErrorsTree}.{etlProcessName.ToLowerInvariant()}";
    }
    
    private static string GetItemErrorsTableName(string etlProcessName)
    {
        return $"{Schemas.EtlItemErrors.EtlItemErrorsTree}.{etlProcessName.ToLowerInvariant()}";
    }
}
