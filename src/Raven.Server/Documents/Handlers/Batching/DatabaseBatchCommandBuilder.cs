using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Batching;

public class DatabaseBatchCommandBuilder : BatchCommandsReader
{
    private readonly DocumentDatabase _database;
    public List<BatchHandler.MergedBatchCommand.AttachmentStream> AttachmentStreams;
    public StreamsTempFile AttachmentStreamsTempFile;

    public DatabaseBatchCommandBuilder(RequestHandler handler, DocumentDatabase database) : base(handler, database.Name, database.IdentityPartsSeparator, BatchRequestParser.Instance)
    {
        _database = database;
    }

    public override async Task SaveStream(JsonOperationContext context, Stream input)
    {
        if (AttachmentStreams == null)
        {
            AttachmentStreams = new List<BatchHandler.MergedBatchCommand.AttachmentStream>();
            AttachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("batch");
        }

        var attachmentStream = new BatchHandler.MergedBatchCommand.AttachmentStream
        {
            Stream = AttachmentStreamsTempFile.StartNewStream()
        };
        attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, input, attachmentStream.Stream, _database.DatabaseShutdown);
        await attachmentStream.Stream.FlushAsync();
        AttachmentStreams.Add(attachmentStream);
    }

    public async Task<BatchHandler.MergedBatchCommand> GetCommand(JsonOperationContext ctx)
    {
        await ExecuteGetIdentities();
        return new BatchHandler.MergedBatchCommand(_database)
        {
            ParsedCommands = Commands,
            AttachmentStreams = AttachmentStreams,
            AttachmentStreamsTempFile = AttachmentStreamsTempFile,
            IsClusterTransaction = IsClusterTransactionRequest
        };
    }
}
