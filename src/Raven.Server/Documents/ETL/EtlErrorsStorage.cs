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
    private const int ErrorsLimitPerEtl = 100;

    private readonly string _errorsTableName = GetErrorsTableName(databaseName);
    private readonly string _partialErrorsTableName = GetPartialErrorsTableName(databaseName);
    
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
        Schemas.EtlErrors.Current.Create(tx, _errorsTableName, 16);
        Schemas.PartialEtlErrors.Current.Create(tx, _partialErrorsTableName, 16);
        
        tx.Commit();
    }
    
    internal void StoreError(EtlError error)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
                
                var createdAtTicks = Bits.SwapBytes(error.CreatedAt.Ticks);
                var affectedDocumentsCountSwapped = Bits.SwapBytes(error.AffectedDocumentsCount);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)error.Step);
                var severitySwapped = Bits.SwapBytes((long)error.Severity);
                
                var id = context.GetLazyString(error.Id);
                var etlTaskName = context.GetLazyString(error.EtlTaskName);
                var message = context.GetLazyString(error.Message);
                
                using (Slice.From(tx.InnerTransaction.Allocator, etlTaskName, out Slice etlTaskNameSlice))
                {
                    if (table.GetCountOfMatchesFor(Schemas.EtlErrors.Current.Indexes[Schemas.EtlErrors.ByEtlTaskName], etlTaskNameSlice) >= ErrorsLimitPerEtl)
                    {
                        DeleteOldestErrorOfTask(error.EtlTaskName);
                    }
                }
                
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add(etlTaskName.Buffer, etlTaskName.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add((byte*)&affectedDocumentsCountSwapped, sizeof(long));
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));
                    tvb.Add((byte*)&severitySwapped, sizeof(long));
                    tvb.Add(message.Buffer, message.Size);

                    table.Set(tvb);
                }
                
                tx.Commit();
            }
        }
    }
    
    internal void StorePartialError(PartialEtlError error)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);

                var createdAtTicks = Bits.SwapBytes(error.CreatedAt.Ticks);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)error.Step);
                var severitySwapped = Bits.SwapBytes((long)error.Severity);

                var id = context.GetLazyString(error.Id);
                var etlTaskName = context.GetLazyString(error.EtlTaskName);
                var documentId = context.GetLazyString(error.DocumentId);

                using (Slice.From(tx.InnerTransaction.Allocator, etlTaskName, out Slice etlTaskNameSlice))
                {
                    if (table.GetCountOfMatchesFor(Schemas.PartialEtlErrors.Current.Indexes[Schemas.PartialEtlErrors.ByEtlTaskName], etlTaskNameSlice) >= ErrorsLimitPerEtl)
                    {
                        DeleteOldestPartialErrorOfTask(error.EtlTaskName);
                    }
                }

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add(etlTaskName.Buffer, etlTaskName.Size);
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
    
    public IDisposable ReadError(string id, out EtlErrorTableValue value)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;

            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            value = GetError(id, tx);
            
            return scope.Delay();
        }
    }
    
    public IDisposable ReadPartialError(string id, out PartialEtlErrorTableValue value)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;

            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            value = GetPartialError(id, tx);
            
            return scope.Delay();
        }
    }
    
    private EtlErrorTableValue GetError(string id, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
        if (table == null)
            return null;

        using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            return ReadError(ref tvr);
        }
    }
    
    private PartialEtlErrorTableValue GetPartialError(string id, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);
        if (table == null)
            return null;

        using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            return ReadPartialError(ref tvr);
        }
    }
    
    private EtlErrorTableValue ReadError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlErrors.EtlErrorsTable.CreatedAtIndex, out _)));
        var etlTaskName = reader.ReadString(Schemas.EtlErrors.EtlErrorsTable.EtlTaskNameIndex);
        var affectedDocumentsCount = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlErrors.EtlErrorsTable.AffectedDocumentsCountIndex, out _));
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlErrors.EtlErrorsTable.StepIndex, out _));
        var severity = Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlErrors.EtlErrorsTable.SeverityIndex, out _));
        var message = reader.ReadString(Schemas.EtlErrors.EtlErrorsTable.MessageIndex);
        
        return new EtlErrorTableValue
        {
            CreatedAt = createdAt,
            EtlTaskName = etlTaskName,
            AffectedDocumentsCount = affectedDocumentsCount,
            Step = step,
            Severity = severity,
            Message = message
        };
    }
    
    private PartialEtlErrorTableValue ReadPartialError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.PartialEtlErrors.PartialEtlErrorsTable.CreatedAtIndex, out _)));
        var etlTaskName = reader.ReadString(Schemas.PartialEtlErrors.PartialEtlErrorsTable.EtlTaskNameIndex);
        var documentId = reader.ReadString(Schemas.PartialEtlErrors.PartialEtlErrorsTable.DocumentIdIndex);
        var step = Bits.SwapBytes(*(long*)reader.Read(Schemas.PartialEtlErrors.PartialEtlErrorsTable.StepIndex, out _));
        var severity = Bits.SwapBytes(*(long*)reader.Read(Schemas.PartialEtlErrors.PartialEtlErrorsTable.SeverityIndex, out _));
        
        return new PartialEtlErrorTableValue
        {
            CreatedAt = createdAt,
            EtlTaskName = etlTaskName,
            DocumentId = documentId,
            Severity = severity,
            Step = step
        };
    }
    
    private IEnumerable<EtlErrorTableValue> ReadErrorsByCreatedAtIndex(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
        if (table == null)
            yield break;

        foreach (var tvr in table.SeekForwardFrom(Schemas.EtlErrors.Current.Indexes[Schemas.EtlErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return ReadError(ref tvr.Result.Reader);
        }
    }
    
    private IEnumerable<PartialEtlErrorTableValue> ReadPartialErrorsByCreatedAtIndex(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);
        if (table == null)
            yield break;

        foreach (var tvr in table.SeekForwardFrom(Schemas.PartialEtlErrors.Current.Indexes[Schemas.PartialEtlErrors.ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return ReadPartialError(ref tvr.Result.Reader);
        }
    }
    
    public IDisposable ReadErrorsOrderedByCreationDate(out IEnumerable<EtlErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadErrorsByCreatedAtIndex(context);

            return scope.Delay();
        }
    }
    
    public IDisposable ReadPartialErrorsOrderedByCreationDate(out IEnumerable<PartialEtlErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadPartialErrorsByCreatedAtIndex(context);

            return scope.Delay();
        }
    }

    public IDisposable ReadErrorsOfTask(string etlTaskName, out IEnumerable<EtlErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadErrorsOfTask(context, etlTaskName);

            return scope.Delay();
        }
    }
    
    public IDisposable ReadPartialErrorsOfTask(string etlTaskName, out IEnumerable<PartialEtlErrorTableValue> errors)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            errors = ReadPartialErrorsOfTask(context, etlTaskName);

            return scope.Delay();
        }
    }
    
    private IEnumerable<EtlErrorTableValue> ReadErrorsOfTask(TransactionOperationContext context, string etlTaskName)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
        if (table == null)
            yield break;

        using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
        {
            foreach (var tvr in table.SeekForwardFrom(Schemas.EtlErrors.Current.Indexes[Schemas.EtlErrors.ByEtlTaskName], taskNameSlice, 0))
            {
                var error = ReadError(ref tvr.Result.Reader);

                if (error.EtlTaskName != etlTaskName)
                    yield break;

                yield return error;
            }
        }
    }
    
    private IEnumerable<PartialEtlErrorTableValue> ReadPartialErrorsOfTask(TransactionOperationContext context, string etlTaskName)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);
        if (table == null)
            yield break;

        using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
        {
            foreach (var tvr in table.SeekForwardFrom(Schemas.PartialEtlErrors.Current.Indexes[Schemas.PartialEtlErrors.ByEtlTaskName], taskNameSlice, 0))
            {
                var error = ReadPartialError(ref tvr.Result.Reader);

                if (error.EtlTaskName != etlTaskName)
                    yield break;

                yield return error;
            }
        }
    }
    
    public void DeleteErrorsOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
            if (table == null)
                return;

            var idsToDelete = new List<string>();
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlErrors.Current.Indexes[Schemas.EtlErrors.ByEtlTaskName], taskNameSlice, 0))
                {
                    var error = ReadError(ref tvr.Result.Reader);

                    if (error.EtlTaskName != etlTaskName)
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

    public void DeletePartialErrorsOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);
            if (table == null)
                return;

            var idsToDelete = new List<string>();
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.PartialEtlErrors.Current.Indexes[Schemas.PartialEtlErrors.ByEtlTaskName], taskNameSlice, 0))
                {
                    var error = ReadPartialError(ref tvr.Result.Reader);

                    if (error.EtlTaskName != etlTaskName)
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

    private void DeleteOldestErrorOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
            if (table == null)
                return;
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.EtlErrors.Current.Indexes[Schemas.EtlErrors.ByEtlTaskName], taskNameSlice, 0))
                {
                    var error = ReadError(ref tvr.Result.Reader);

                    if (error.EtlTaskName != etlTaskName)
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

    private void DeleteOldestPartialErrorOfTask(string etlTaskName)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenWriteTransaction());

            var table = context.Transaction.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);
            if (table == null)
                return;
            
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, etlTaskName, out Slice taskNameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(Schemas.PartialEtlErrors.Current.Indexes[Schemas.PartialEtlErrors.ByEtlTaskName], taskNameSlice, 0))
                {
                    var error = ReadPartialError(ref tvr.Result.Reader);

                    if (error.EtlTaskName != etlTaskName)
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

    private static string GetErrorsTableName(string databaseName)
    {
        return $"{Schemas.EtlErrors.EtlErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
    
    private static string GetPartialErrorsTableName(string databaseName)
    {
        return $"{Schemas.PartialEtlErrors.PartialEtlErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
}
