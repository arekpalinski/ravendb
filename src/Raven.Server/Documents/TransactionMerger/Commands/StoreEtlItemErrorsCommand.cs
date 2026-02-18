using System;
using System.Collections.Generic;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public class StoreEtlItemErrorsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction> 
{
    private readonly List<EtlItemError> _itemErrors;
    private readonly string _tableName;

    public StoreEtlItemErrorsCommand(List<EtlItemError> itemErrors, string tableName)
    {
        _itemErrors = itemErrors;
        _tableName = tableName;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        EtlErrorsStorage.StoreItemErrors(context, _itemErrors, _tableName);
        return 1;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        throw new NotImplementedException();
    }
}
