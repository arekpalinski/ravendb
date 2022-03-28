using System;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public class ClusterTransactionMergedCommandDto : TransactionOperationsMerger.IReplayableCommandDto<ClusterTransactionMergedCommand>
{
    public ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand> Batch { get; set; }

    public ClusterTransactionMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        var command = new ClusterTransactionMergedCommand(database, Batch);
        return command;
    }
}
