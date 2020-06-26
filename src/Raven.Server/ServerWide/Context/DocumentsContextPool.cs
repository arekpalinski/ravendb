// -----------------------------------------------------------------------
//  <copyright file="DocumentsContextPool.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsContextPool : JsonContextPoolBase<DocumentsOperationContext>, IDocumentsContextPool
    {
        private DocumentsStorage _documentsStorage;

        public DocumentsContextPool(DocumentsStorage documentsStorage)
            : base(documentsStorage.Configuration.Memory.MaxContextSizeToKeep)
        {
            _documentsStorage = documentsStorage;
        }

        protected override DocumentsOperationContext CreateContext()
        {
            return _documentsStorage.Is32Bits ?
                new DocumentsOperationContext(_documentsStorage, 32 * 1024, 4 * 1024, 2 * 1024, LowMemoryFlag) :
                new DocumentsOperationContext(_documentsStorage, 64 * 1024, 16 * 1024, 8 * 1024, LowMemoryFlag);
        }

        public override void Dispose()
        {
            _documentsStorage = null;
            base.Dispose();
        }
    }
}
