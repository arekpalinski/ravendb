using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations.BulkInsert;

internal class ShardedBulkInsertOperation : BulkInsertOperationBase, IShardedOperation<HttpResponseMessage>, IAsyncDisposable
{
    private readonly bool _skipOverwriteIfUnchanged;
    private readonly ShardedContext _shardedContext;
    private readonly BulkInsertOperation.BulkInsertStreamExposerContent[] _streamExposerPerShard;

    public ShardedBulkInsertOperation(long id, bool skipOverwriteIfUnchanged, ShardedContext shardedContext)
    {
        _operationId = id;
        _skipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
        _shardedContext = shardedContext;

        _streamExposerPerShard = new BulkInsertOperation.BulkInsertStreamExposerContent[shardedContext.ShardCount];

        for (int i = 0; i < _streamExposerPerShard.Length; i++)
        {
            _streamExposerPerShard[i] = new BulkInsertOperation.BulkInsertStreamExposerContent();
        }
    }

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.NoCompression;

    public Stream[] RequestBodyStreamPerShard { get; private set; }

    public HttpResponseMessage Combine(Memory<HttpResponseMessage> results) => null;

    public RavenCommand<HttpResponseMessage> CreateCommandForShard(int shardNumber)
    {
        return new BulkInsertOperation.BulkInsertCommand(_operationId, _streamExposerPerShard[shardNumber], null, _skipOverwriteIfUnchanged);
    }

    protected override bool HasStream => RequestBodyStreamPerShard != null;

    protected override Task WaitForId()
    {
        return Task.CompletedTask;
    }

    public async Task StoreAsync()
    {
        await ExecuteBeforeStore();
    }

    protected override async Task EnsureStream()
    {
        /*if (CompressionLevel != CompressionLevel.NoCompression)
            _streamExposerContent.Headers.ContentEncoding.Add("gzip");

        var bulkCommand = new BulkInsertCommand(
            _operationId,
            _streamExposerContent,
            _nodeTag,
            _options.SkipOverwriteIfUnchanged);

        _bulkInsertExecuteTask = ExecuteAsync(bulkCommand);

        _stream = await _streamExposerContent.OutputStream.ConfigureAwait(false);

        _requestBodyStream = _stream;
        if (CompressionLevel != CompressionLevel.NoCompression)
        {
            _compressedStream = new GZipStream(_stream, CompressionLevel, leaveOpen: true);
            _requestBodyStream = _compressedStream;
        }

        _currentWriter.Write('[');*/

        _bulkInsertExecuteTask = _shardedContext.ShardExecutor.ExecuteParallelForAllAsync(this);

        await Task.WhenAll(_streamExposerPerShard.Select(x => x.OutputStream));

        RequestBodyStreamPerShard = new Stream[_streamExposerPerShard.Length];

        for (int i = 0; i < _streamExposerPerShard.Length; i++)
        {
            var stream = await _streamExposerPerShard[i].OutputStream;

            if (CompressionLevel != CompressionLevel.NoCompression)
            {
                stream = new GZipStream(stream, CompressionLevel, leaveOpen: true);
            }

            RequestBodyStreamPerShard[i] = stream;
        }
    }

    protected override async Task<BulkInsertAbortedException> GetExceptionFromOperation()
    {
        // TODO arek
        var getStateOperation = new GetShardedOperationStateOperation(_operationId);
        var result = await _shardedContext.ShardExecutor.ExecuteParallelForAllAsync(getStateOperation);

        if (!(result?.Result is OperationExceptionResult error))
            return null;
        return new BulkInsertAbortedException(error.Error);
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException(); // TODO arek
    }
}
