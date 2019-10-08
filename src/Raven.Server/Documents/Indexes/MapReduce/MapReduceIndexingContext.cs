using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MapReduceResultsStore>("MapReduceIndexingContext");

        internal static Slice LastMapResultIdKey;

        internal static Slice PrefixesOfReduceOutputDocumentsToDeleteKey;

        public FixedSizeTree DocumentMapEntries;

        public Tree MapPhaseTree;
        public Tree ReducePhaseTree;
        public Tree ReduceOutputsTree;

        public FixedSizeTree ResultsStoreTypes;
        public Dictionary<ulong, MapReduceResultsStore> StoreByReduceKeyHash = new Dictionary<ulong, MapReduceResultsStore>(NumericEqualityComparer.BoxedInstanceUInt64);
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();
        public readonly HashSet<long> FreedPages = new HashSet<long>();
        public HashSet<string> _prefixesOfReduceOutputDocumentsToDelete;

        public long NextMapResultId;

        static MapReduceIndexingContext()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "__raven/map-reduce/#next-map-result-id", ByteStringType.Immutable, out LastMapResultIdKey);
                Slice.From(ctx, "__raven/map-reduce/#prefixes-of-reduce-output-documents-to-delete", ByteStringType.Immutable, out PrefixesOfReduceOutputDocumentsToDeleteKey);
            }
        }

        public void Dispose()
        {
            DocumentMapEntries?.Dispose();
            DocumentMapEntries = null;
            MapPhaseTree = null;
            ReducePhaseTree = null;
            ReduceOutputsTree = null;
            ProcessedDocEtags.Clear();
            ProcessedTombstoneEtags.Clear();
            StoreByReduceKeyHash.Clear();
            FreedPages.Clear();
        }

        public unsafe void StoreNextMapResultId()
        {
            if (MapPhaseTree.Llt.Environment.Options.IsCatastrophicFailureSet)
                return; // avoid re-throwing it

            try
            {
                using (MapPhaseTree.DirectAdd(LastMapResultIdKey, sizeof(long), out byte* ptr))
                    *(long*)ptr = NextMapResultId;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to store next map result id", e);

                throw;
            }
        }

        public void AddPrefixOfReduceOutputDocumentsToDelete(Tree reduceOutputsTree, string prefix)
        {
            reduceOutputsTree.MultiAdd(PrefixesOfReduceOutputDocumentsToDeleteKey, prefix);

            if (_prefixesOfReduceOutputDocumentsToDelete == null)
                _prefixesOfReduceOutputDocumentsToDelete = new HashSet<string>();

            _prefixesOfReduceOutputDocumentsToDelete.Add(prefix);
        }

        public void DeletePrefixOfReduceOutputDocumentsToDelete(string prefix)
        {
            ReduceOutputsTree.MultiDelete(PrefixesOfReduceOutputDocumentsToDeleteKey, prefix);

            _prefixesOfReduceOutputDocumentsToDelete.Remove(prefix);
        }

        public unsafe void Initialize(Tree mapEntriesTree, Tree reduceOutputsTree)
        {
            if (mapEntriesTree != null)
            {
                var read = mapEntriesTree.Read(LastMapResultIdKey);

                if (read == null)
                    return;

                NextMapResultId = *(long*)read.Reader.Base;
            }

            if (reduceOutputsTree != null)
            {
                using (var it = ReduceOutputsTree.MultiRead(PrefixesOfReduceOutputDocumentsToDeleteKey))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        _prefixesOfReduceOutputDocumentsToDelete = new HashSet<string>();

                        do
                        {
                            _prefixesOfReduceOutputDocumentsToDelete.Add(it.CurrentKey.ToString());

                        } while (it.MoveNext());
                    }
                }
            }
        }

        public bool HasPrefixesOfReduceOutputDocumentsToDelete()
        {
            return _prefixesOfReduceOutputDocumentsToDelete != null && _prefixesOfReduceOutputDocumentsToDelete.Count > 0;
        }

        public bool HasPrefixesOfReduceOutputDocumentsToDelete(TransactionOperationContext indexContext)
        {
            indexContext.Transaction.InnerTransaction.ReadTree()
            return _prefixesOfReduceOutputDocumentsToDelete != null && _prefixesOfReduceOutputDocumentsToDelete.Count > 0;
        }

        public HashSet<string> GetPrefixesOfReduceOutputDocumentsToDelete()
        {
            return _prefixesOfReduceOutputDocumentsToDelete;
        }
    }
}
