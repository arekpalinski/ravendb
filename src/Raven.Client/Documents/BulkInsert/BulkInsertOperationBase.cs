using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.BulkInsert;

namespace Raven.Client.Documents.BulkInsert;

public abstract class BulkInsertOperationBase
{
    // TODO arek - rename

    protected Task _bulkInsertExecuteTask;
    protected long _operationId = -1;

    protected abstract bool HasStream { get; }

    protected async Task ExecuteBeforeStore()
    {
        if (HasStream == false)
        {
            await WaitForId().ConfigureAwait(false);
            await EnsureStream().ConfigureAwait(false);
        }

        if (_bulkInsertExecuteTask.IsFaulted)
        {
            try
            {
                await _bulkInsertExecuteTask.ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await ThrowBulkInsertAborted(e).ConfigureAwait(false);
            }
        }
    }

    protected async Task ThrowBulkInsertAborted(Exception e, Exception flushEx = null)
    {
        var errors = new List<Exception>(3);

        try
        {
            var error = await GetExceptionFromOperation().ConfigureAwait(false);

            if (error != null)
                errors.Add(error);
        }
        catch (Exception exceptionFromOperation)
        {
            errors.Add(exceptionFromOperation);
        }

        if (flushEx != null)
            errors.Add(flushEx);

        errors.Add(e);

        throw new BulkInsertAbortedException("Failed to execute bulk insert", new AggregateException(errors));
    }

    protected abstract Task WaitForId();

    protected abstract Task EnsureStream();

    protected abstract Task<BulkInsertAbortedException> GetExceptionFromOperation();
}
