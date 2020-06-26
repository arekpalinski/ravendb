using System;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Revisions
{
    public class RevisionsOperations
    {
        internal class DeleteRevisionsBeforeCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly string _collection;
            private readonly DateTime _time;
            private readonly DocumentDatabase _database;

            public DeleteRevisionsBeforeCommand(string collection, DateTime time, DocumentDatabase database)
            {
                _collection = collection;
                _time = time;
                _database = database;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                _database.DocumentsStorage.RevisionsStorage.DeleteRevisionsBefore(context, _collection, _time);
                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new DeleteRevisionsBeforeCommandDto
                {
                    Collection = _collection,
                    //TODO To consider what should be the result because while replaying, the revisions are newer then the date of recorded DeleteRevisionsBeforeCommand 
                    Time = _time
                };
            }
        }
    }

    internal class DeleteRevisionsBeforeCommandDto : TransactionOperationsMerger.IReplayableCommandDto<RevisionsOperations.DeleteRevisionsBeforeCommand>
    {
        public string Collection;
        public DateTime Time;

        public RevisionsOperations.DeleteRevisionsBeforeCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new RevisionsOperations.DeleteRevisionsBeforeCommand(Collection, Time, database);
            return command;
        }
    }
}
