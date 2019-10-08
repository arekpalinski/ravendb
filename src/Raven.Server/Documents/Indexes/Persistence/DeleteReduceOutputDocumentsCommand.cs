using System;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public class DeleteReduceOutputDocumentsCommand : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly DocumentDatabase _database;
        private readonly string _documentsPrefix;
        private readonly int _batchSize;

        public DeleteReduceOutputDocumentsCommand(DocumentDatabase database, string documentsPrefix, int batchSize)
        {
            _database = database;
            _documentsPrefix = documentsPrefix;
            _batchSize = batchSize;
        }

        public long DeleteCount { get; set; }

        protected override int ExecuteCmd(DocumentsOperationContext context)
        {
            var deleteResults = _database.DocumentsStorage.DeleteDocumentsStartingWith(context, _documentsPrefix, _batchSize);

            DeleteCount = deleteResults.Count;

            return deleteResults.Count;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
