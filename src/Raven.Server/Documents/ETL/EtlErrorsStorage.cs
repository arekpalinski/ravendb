using System;
using System.Collections.Generic;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.ETL;

public unsafe class EtlErrorsStorage(string databaseName)
{
    private readonly string _errorsTableName = GetErrorsTableName(databaseName);
    private readonly string _partialErrorsTableName = GetPartialErrorsTableName(databaseName);

    // todo what do we want to log?
    private readonly Logger _logger = LoggingSource.Instance.GetLogger<NotificationsStorage>(databaseName);

    protected StorageEnvironment Environment;

    protected TransactionContextPool ContextPool;
    
    public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
    {
        Environment = environment;
        ContextPool = contextPool;

        using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            CreateSchema(context);
        }
    }
    
    private void CreateSchema(TransactionOperationContext context)
    {
        Schemas.EtlErrors.Current.Create(context.Transaction.InnerTransaction, _errorsTableName, 16);
        Schemas.PartialEtlErrors.Current.Create(context.Transaction.InnerTransaction, _partialErrorsTableName, 16);
    }
    
    internal void StoreError(EtlError error)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
                
                var createdAtTicks = Bits.SwapBytes(error.CreatedAt.Ticks);
                var affectedDocumentsCountSwapped = Bits.SwapBytes(error.AffectedDocumentsCount);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)error.Type);
                
                var id = context.GetLazyString(error.Id);
                
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add((byte*)&affectedDocumentsCountSwapped, sizeof(long));
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));

                    table.Set(tvb);
                }
            }
        }
    }
    
    /*
    internal void StoreError(LazyStringValue id, DateTime createdAt, long affectedDocumentsCount, EtlErrorType etlErrorType)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.EtlErrors.Current, _errorsTableName);
                
                var createdAtTicks = Bits.SwapBytes(createdAt.Ticks);
                var affectedDocumentsCountSwapped = Bits.SwapBytes(affectedDocumentsCount);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)etlErrorType);
                
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add((byte*)&affectedDocumentsCountSwapped, sizeof(long));
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));

                    table.Set(tvb);
                }
            }
        }
    }
    */
    
    internal void StorePartialError(PartialEtlError error)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);

                var createdAtTicks = Bits.SwapBytes(error.CreatedAt.Ticks);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)error.Type);

                var id = context.GetLazyString(error.Id);
                var documentId = context.GetLazyString(error.DocumentId);
                
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add(documentId.Buffer, documentId.Size);
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));

                    table.Set(tvb);
                }
            }
        }
    }
    
    /*
    internal void StorePartialError(LazyStringValue id, DateTime createdAt, LazyStringValue documentId, EtlErrorType etlErrorType)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _partialErrorsTableName);

                var createdAtTicks = Bits.SwapBytes(createdAt.Ticks);
                var etlErrorTypeSwapped = Bits.SwapBytes((long)etlErrorType);
                
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add(documentId.Buffer, documentId.Size);
                    tvb.Add((byte*)&etlErrorTypeSwapped, sizeof(long));

                    table.Set(tvb);
                }
            }
        }
    }
    */
    
    public EtlErrorTableValue ReadError(string id)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;

            scope.EnsureDispose(ContextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            return GetError(id, tx);
        }
    }
    
    public PartialEtlErrorTableValue ReadPartialError(string id)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;

            scope.EnsureDispose(ContextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            return GetPartialError(id, tx);
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
        var table = tx.InnerTransaction.OpenTable(Schemas.PartialEtlErrors.Current, _errorsTableName);
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
        
        return new EtlErrorTableValue
        {
            CreatedAt = createdAt
        };
    }
    
    private PartialEtlErrorTableValue ReadPartialError(ref TableValueReader reader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(Schemas.EtlErrors.EtlErrorsTable.CreatedAtIndex, out _)));
        
        return new PartialEtlErrorTableValue
        {
            CreatedAt = createdAt
        };
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
    
    private static string GetErrorsTableName(string databaseName)
    {
        return $"{Schemas.EtlErrors.EtlErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
    
    private static string GetPartialErrorsTableName(string databaseName)
    {
        return $"{Schemas.PartialEtlErrors.PartialEtlErrorsTree}.{databaseName.ToLowerInvariant()}";
    }
}
