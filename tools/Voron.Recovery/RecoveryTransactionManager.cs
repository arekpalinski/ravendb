using System;
using System.Diagnostics;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Platform;
using Voron.Impl;

namespace Voron.Recovery
{
    public class RecoveryTransactionManager : IDisposable
    {
        private readonly DocumentsOperationContext _context;
        private TransactionBatch _writeTxBatch;
        private long _maybePulseCounter;

        public RecoveryTransactionManager(DocumentsOperationContext context)
        {
            _context = context;
        }

        public IDisposable EnsureTransaction()
        {
            if (_writeTxBatch == null || _writeTxBatch.Disposed)
                _writeTxBatch = new TransactionBatch(_context.OpenWriteTransaction());

            return _writeTxBatch.GetScope();
        }

        public bool MaybePulseTransaction(int iterationInterval = 128)
        {
            if (++_maybePulseCounter % iterationInterval != 0)
                return false;

            if (_writeTxBatch.CanContinueBatch())
                return false;

            PulseTransaction();
            return true;
        }

        public void PulseTransaction()
        {
            _writeTxBatch.Dispose(forceCommit: true);
        }

        public void Dispose()
        {
            _writeTxBatch?.Dispose(forceCommit: true);
        }

        private class TransactionBatch : IDisposable
        {
            private readonly Size _scratchSpaceLimit = new Size(512, SizeUnit.Megabytes);
            private readonly Size _mappedMemorySpaceLimitOn32Bits = new Size(128, SizeUnit.Megabytes);
            private bool _forceCommit;
            private int _nestingLevel;

            private readonly DocumentsTransaction _tx;

            public TransactionBatch(DocumentsTransaction tx)
            {
                Debug.Assert(tx.InnerTransaction.IsWriteTransaction, "tx.InnerTransaction.IsWriteTransaction");

                _tx = tx;
            }

            public bool CanContinueBatch()
            {
                if (_forceCommit)
                    return false;

                var llt = _tx.InnerTransaction.LowLevelTransaction;

                if (llt.Environment.Options.ScratchSpaceUsage.ScratchSpaceInBytes > _scratchSpaceLimit.GetValue(SizeUnit.Bytes))
                    return false;

                if (PlatformDetails.Is32Bits)
                {
                    if (llt.GetTotal32BitsMappedSize() > _mappedMemorySpaceLimitOn32Bits)
                        return false;
                }

                // TODO arek - if database is encrypted then check LowLevelTransaction.TotalEncryptionBufferSize;

                return true;
            }

            public bool Disposed { get; private set; }

            public IDisposable GetScope()
            {
                return new BatchScope(this);
            }

            public void Dispose()
            {
                if (Disposed)
                    return;

                if (CanContinueBatch())
                    return;

                try
                {
                    _tx.Commit();
                }
                finally
                {
                    Disposed = true;

                    _tx.Dispose();
                }
            }

            public void Dispose(bool forceCommit)
            {
                _forceCommit = forceCommit;
                Dispose();
            }

            private class BatchScope : IDisposable
            {
                private readonly TransactionBatch _batch;

                public BatchScope(TransactionBatch batch)
                {
                    _batch = batch;
                    _batch._nestingLevel++;
                }

                public void Dispose()
                {
                    if (--_batch._nestingLevel == 0)
                    {
                        _batch.Dispose();
                    }
                }
            }
        }
    }
}
