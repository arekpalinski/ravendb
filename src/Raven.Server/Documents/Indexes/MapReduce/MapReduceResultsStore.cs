using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class MapReduceResultsStore : IDisposable
    {
        public const string ReduceTreePrefix = "__raven/map-reduce/#reduce-tree-";
        public const string NestedValuesPrefix = "__raven/map-reduce/#nested-section-";

        private readonly ulong _reduceKeyHash;
        private readonly TransactionOperationContext _indexContext;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly Slice _nestedValueKey;
        private ByteStringContext.InternalScope _nestedValueKeyScope;
        private readonly Transaction _tx;

        private NestedMapResultsSection _nestedSection;

        public MapResultsStorageType Type { get; private set; }

        public Tree Tree;
        public readonly HashSet<long> ModifiedPages;
        

        public MapReduceResultsStore(ulong reduceKeyHash, MapResultsStorageType type, TransactionOperationContext indexContext, MapReduceIndexingContext mapReduceContext, bool create)
        {
            _reduceKeyHash = reduceKeyHash;
            Type = type;
            _indexContext = indexContext;
            _mapReduceContext = mapReduceContext;
            _tx = indexContext.Transaction.InnerTransaction;

            ModifiedPages = new HashSet<long>();

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    InitializeTree(create);
                    break;
                case MapResultsStorageType.Nested:
                    _nestedValueKeyScope = Slice.From(indexContext.Allocator, NestedValuesPrefix + reduceKeyHash, ByteStringType.Immutable, out _nestedValueKey);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private readonly HashSet<string> _alreadyInitializedTrees = new HashSet<string>();

        private void InitializeTree(bool create)
        {
            var treeName = ReduceTreePrefix + _reduceKeyHash;
            var options = _tx.LowLevelTransaction.Environment.Options.RunningOn32Bits ? TreeFlags.None : TreeFlags.LeafsCompressed;
            Tree = create ? _tx.CreateTree(treeName, flags: options) : _tx.ReadTree(treeName);

            if (!_alreadyInitializedTrees.Contains(treeName))
            {
                Tree.PageModified += (page, flags) =>
                {
                    if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                        return;

                    ModifiedPages.Add(page);
                    _mapReduceContext.FreedPages.Remove(page);
                };

                Tree.PageFreed += (page, flags) =>
                {
                    if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                        return;

                    _mapReduceContext.FreedPages.Add(page);
                    ModifiedPages.Remove(page);
                };

                _alreadyInitializedTrees.Add(treeName);
            }
        }

        public void Delete(long id)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;

                    Debugging();

                    using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                        Tree.Delete(entrySlice);
                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    section.Delete(id);

                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        public void Add(long id, BlittableJsonReaderObject result)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    
                    using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                    {
                        Debugging();

                        using (Tree.DirectAdd(entrySlice, result.Size, out byte* ptr))
                            result.CopyTo(ptr);
                    }

                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    if (_mapReduceContext.ReducePhaseTree.ShouldGoToOverflowPage(_nestedSection.SizeAfterAdding(result)))
                    {
                        // would result in an overflow, that would be a space waste anyway, let's move to tree mode
                        MoveExistingResultsToTree(section);
                        Add(Bits.SwapBytes(id), result); // now re-add the value, revert id to its original value
                    }
                    else
                    {
                        section.Add(id, result);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void Debugging()
        {
            if (Tree.Name.ToString() == "__raven/map-reduce/#reduce-tree-7258129073431256551")
            {
                Page p_12272 = Tree.GetReadOnlyPage(12272);
                var treePage_12272 = new TreePage(p_12272.Pointer, Constants.Storage.PageSize);


                Page p_13271 = Tree.GetReadOnlyPage(13271);
                var treePage_13271 = new TreePage(p_12272.Pointer, Constants.Storage.PageSize);

                Tree.ValidateTree_Forced(Tree.State.RootPageNumber);
                Tree.DebugValidateBranchReferences();

                DebugStuff.RenderAndShow(Tree);
                DebugStuff.RenderAndShow(Tree, decompress: true);

                var problematicId = 6564595;

                Slice.External(_indexContext.Allocator, (byte*)&problematicId, sizeof(long), out var problematicIdSlice);

                TreePage findPageFor = Tree.FindPageFor(problematicIdSlice, out var node);

                //DebugStuff.RenderAndShow(Tree);
            }
        }

        public ReadMapEntryScope Get(long id)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:

                    Debugging();

                    Slice entrySlice;
                    using (Slice.External(_indexContext.Allocator, (byte*)&id, sizeof(long), out entrySlice))
                    {
                        if (Tree.IsLeafCompressionSupported)
                        {
                            var read = Tree.ReadDecompressed(entrySlice);
                            if (read == null)
                                throw new InvalidOperationException($"Could not find a map result with id '{id}' in '{Tree.Name}' tree");
                            return new ReadMapEntryScope(read);
                        }
                        else
                        {
                            var read = Tree.Read(entrySlice);
                            if (read == null)
                                throw new InvalidOperationException($"Could not find a map result with id '{id}' in '{Tree.Name}' tree");
                            return new ReadMapEntryScope(PtrSize.Create(read.Reader.Base, read.Reader.Length));
                        }
                    }
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();
                    return new ReadMapEntryScope(section.Get(id));
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void MoveExistingResultsToTree(NestedMapResultsSection section)
        {
            Type = MapResultsStorageType.Tree;

            var byteType = (byte)Type;

            using (Slice.External(_indexContext.Allocator, &byteType, sizeof(byte), out Slice val))
                _mapReduceContext.ResultsStoreTypes.Add((long)_reduceKeyHash, val);
            InitializeTree(create: true);

            section.MoveTo(Tree);
        }

        public NestedMapResultsSection GetNestedResultsSection(Tree tree = null)
        {
            if (_nestedSection != null)
                return _nestedSection;

            _nestedSection = new NestedMapResultsSection(_indexContext.Environment, tree ?? _mapReduceContext.ReducePhaseTree, _nestedValueKey);

            return _nestedSection;
        }

        public void Dispose()
        {
            _nestedValueKeyScope.Dispose();
            Tree?.Dispose();
        }
    }
}
