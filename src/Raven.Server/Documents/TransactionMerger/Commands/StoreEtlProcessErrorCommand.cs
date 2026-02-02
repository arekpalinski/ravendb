using System;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public sealed class StoreEtlProcessErrorCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction> 
{
    private readonly EtlProcessError _processError;
    private readonly string _tableName;

    public StoreEtlProcessErrorCommand(EtlProcessError processError, string tableName)
    {
        _processError = processError;
        _tableName = tableName;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        EtlErrorsStorage.StoreProcessError(context, _processError, _tableName);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}
