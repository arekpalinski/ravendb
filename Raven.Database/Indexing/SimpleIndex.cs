//-----------------------------------------------------------------------
// <copyright file="SimpleIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Util;
using Spatial4n.Core.Exceptions;

namespace Raven.Database.Indexing
{
	internal class SimpleIndex : Index
	{
		public SimpleIndex(Directory directory, int id, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator, WorkContext context)
			: base(directory, id, indexDefinition, viewGenerator, context)
		{
		}

		public override bool IsMapReduce
		{
			get { return false; }
		}

		public DateTime LastCommitPointStoreTime { get; private set; }

		public override IndexingPerformanceStats IndexDocuments(AbstractViewGenerator viewGenerator, IndexingBatch batch, IStorageActionsAccessor actions, DateTime minimumTimestamp)
		{
			var count = 0;
			var sourceCount = 0;
			var flushToDiskDuration = new Stopwatch();
			var recreateSearcherDuration = new Stopwatch();

			IndexingPerformanceStats performance = null;
			var performanceStats = new List<BasePefromanceStats>();

			Write((indexWriter, analyzer, stats) =>
			{
				var processedKeys = new HashSet<string>();
				var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(indexId))
					.Where(x => x != null)
					.ToList();

				try
				{
					performance = RecordCurrentBatch("Current", "Index", batch.Docs.Count);

					var deleteExistingDocumentsDuration = new Stopwatch();
					var docIdTerm = new Term(Constants.DocumentIdFieldName);
					var documentsWrapped = batch.Docs.Select((doc, i) =>
					{
						Interlocked.Increment(ref sourceCount);
						if (doc.__document_id == null)
							throw new ArgumentException(
								string.Format("Cannot index something which doesn't have a document id, but got: '{0}'", doc));

						string documentId = doc.__document_id.ToString();
						if (processedKeys.Add(documentId) == false)
							return doc;

						InvokeOnIndexEntryDeletedOnAllBatchers(batchers, docIdTerm.CreateTerm(documentId.ToLowerInvariant()));

						if (batch.SkipDeleteFromIndex[i] == false ||
						    context.ShouldRemoveFromIndex(documentId)) // maybe it is recently deleted?
						{
							using (StopwatchScope.For(deleteExistingDocumentsDuration))
							{
								indexWriter.DeleteDocuments(docIdTerm.CreateTerm(documentId.ToLowerInvariant()));
							}
						}

						return doc;
					})
					.Where(x => x is FilteredDocument == false)
					.ToList();

					performanceStats.Add(new PerformanceStats
					{
						Name = IndexingOperation.Lucene_DeleteExistingDocument,
						DurationMs = deleteExistingDocumentsDuration.ElapsedMilliseconds
					});

					var allReferencedDocs = new ConcurrentQueue<IDictionary<string, HashSet<string>>>();
					var allReferenceEtags = new ConcurrentQueue<IDictionary<string, Etag>>();

					var parallelOperations = new ConcurrentQueue<ParallelBatchStats>();

					var parallelProcessingStart = SystemTime.UtcNow;

					BackgroundTaskExecuter.Instance.ExecuteAllBuffered(context, documentsWrapped, (partition) =>
					{
						var parallelStats = new ParallelBatchStats
						{
							StartDelay = (long) (SystemTime.UtcNow - parallelProcessingStart).TotalMilliseconds
						};

						var anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(context.Database, indexDefinition, viewGenerator, logIndexing);
						var luceneDoc = new Document();
						var documentIdField = new Field(Constants.DocumentIdFieldName, "dummy", Field.Store.YES,
														Field.Index.NOT_ANALYZED_NO_NORMS);

						using (CurrentIndexingScope.Current = new CurrentIndexingScope(context.Database, PublicName))
						{
							string currentDocId = null;
							int outputPerDocId = 0;
							Action<Exception, object> onErrorFunc;
							bool skipDocument = false;

							var linqExecutionDuration = new Stopwatch();
							var addDocumentDutation = new Stopwatch();
							var convertToLuceneDocumentDuration = new Stopwatch();

							foreach (var doc in RobustEnumerationIndex(partition, viewGenerator.MapDefinitions, stats, out onErrorFunc, linqExecutionDuration))
							{
								float boost;
								IndexingResult indexingResult;
								using (StopwatchScope.For(convertToLuceneDocumentDuration))
								{
									try
									{

										indexingResult = GetIndexingResult(doc, anonymousObjectToLuceneDocumentConverter, out boost);
									}
									catch (Exception e)
									{
										onErrorFunc(e, doc);
										continue;
									}
								}

								// ReSharper disable once RedundantBoolCompare --> code clarity
								if (indexingResult.NewDocId == null || indexingResult.ShouldSkip != false)
								{
									continue;
								}
								if (currentDocId != indexingResult.NewDocId)
								{
									currentDocId = indexingResult.NewDocId;
									outputPerDocId = 0;
									skipDocument = false;
								}
								if (skipDocument)
									continue;
								outputPerDocId++;
								if (EnsureValidNumberOfOutputsForDocument(currentDocId, outputPerDocId) == false)
								{
									skipDocument = true;
									continue;
								}
								Interlocked.Increment(ref count);

								using (StopwatchScope.For(convertToLuceneDocumentDuration))
								{
									luceneDoc.GetFields().Clear();
									luceneDoc.Boost = boost;
									documentIdField.SetValue(indexingResult.NewDocId.ToLowerInvariant());
									luceneDoc.Add(documentIdField);
									foreach (var field in indexingResult.Fields)
									{
										luceneDoc.Add(field);
									}
								}

								batchers.ApplyAndIgnoreAllErrors(
									exception =>
									{
										logIndexing.WarnException(
										string.Format(
											"Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
											indexId, indexingResult.NewDocId),
											exception);
										context.AddError(indexId,
															 indexingResult.NewDocId,
															 exception.Message,
															 "OnIndexEntryCreated Trigger"
												);
									},
									trigger => trigger.OnIndexEntryCreated(indexingResult.NewDocId, luceneDoc));
								LogIndexedDocument(indexingResult.NewDocId, luceneDoc);

								using (StopwatchScope.For(addDocumentDutation))
								{
									AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
								}

								Interlocked.Increment(ref stats.IndexingSuccesses);
							}
							allReferenceEtags.Enqueue(CurrentIndexingScope.Current.ReferencesEtags);
							allReferencedDocs.Enqueue(CurrentIndexingScope.Current.ReferencedDocuments);

							parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.LoadDocument, CurrentIndexingScope.Current.LoadDocumentDuration.ElapsedMilliseconds));

							parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Linq_MapExecution, linqExecutionDuration.ElapsedMilliseconds));
							parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Lucene_ConvertToLuceneDocument, convertToLuceneDocumentDuration.ElapsedMilliseconds));
							parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Lucene_AddDocument, addDocumentDutation.ElapsedMilliseconds));

							parallelOperations.Enqueue(parallelStats);
						}
					});

					performanceStats.Add(new ParallelPefromanceStats
					{
						NumberOfThreads = parallelOperations.Count,
						DurationMs = (long) (SystemTime.UtcNow - parallelProcessingStart).TotalMilliseconds,
						BatchedOperations = parallelOperations.ToList()
					});

					UpdateDocumentReferences(actions, allReferencedDocs, allReferenceEtags);
				}
				catch (Exception e)
				{
					batchers.ApplyAndIgnoreAllErrors(
						ex =>
						{
							logIndexing.WarnException("Failed to notify index update trigger batcher about an error", ex);
							context.AddError(indexId, null, ex.Message, "AnErrorOccured Trigger");
						},
						x => x.AnErrorOccured(e));
					throw;
				}
				finally
				{
					batchers.ApplyAndIgnoreAllErrors(
						e =>
						{
							logIndexing.WarnException("Failed to dispose on index update trigger", e);
							context.AddError(indexId, null, e.Message, "Dispose Trigger");
						},
						x => x.Dispose());
				}
				return new IndexedItemsInfo(batch.HighestEtagBeforeFiltering)
				{
					ChangedDocs = sourceCount
				};
			}, flushToDiskDuration, recreateSearcherDuration);

			performanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_FlushToDisk, flushToDiskDuration.ElapsedMilliseconds));
			performanceStats.Add(PerformanceStats.From(IndexingOperation.Lucene_RecreateSearcher, recreateSearcherDuration.ElapsedMilliseconds));

			BatchCompleted("Current", "Index", sourceCount, count, performanceStats);

			logIndexing.Debug("Indexed {0} documents for {1}", count, indexId);

			return performance;
		}

		protected override bool IsUpToDateEnoughToWriteToDisk(Etag highestETag)
		{
			bool upToDate = false;
			context.Database.TransactionalStorage.Batch(accessor =>
			{
				upToDate = accessor.Staleness.GetMostRecentDocumentEtag() == highestETag;
			});
			return upToDate;
		}

		protected override void HandleCommitPoints(IndexedItemsInfo itemsInfo, IndexSegmentsInfo segmentsInfo)
		{
			if (ShouldStoreCommitPoint(itemsInfo) && itemsInfo.HighestETag != null)
			{
				context.IndexStorage.StoreCommitPoint(indexId.ToString(), new IndexCommitPoint
				{
					HighestCommitedETag = itemsInfo.HighestETag,
					TimeStamp = LastIndexTime,
					SegmentsInfo = segmentsInfo ?? IndexStorage.GetCurrentSegmentsInfo(indexDefinition.Name, directory)
				});

				LastCommitPointStoreTime = SystemTime.UtcNow;
			}
			else if (itemsInfo.DeletedKeys != null && directory is RAMDirectory == false)
			{
				context.IndexStorage.AddDeletedKeysToCommitPoints(indexDefinition, itemsInfo.DeletedKeys);
			}
		}

		private bool ShouldStoreCommitPoint(IndexedItemsInfo itemsInfo)
		{
			if (itemsInfo.DisableCommitPoint)
				return false;

			if (directory is RAMDirectory) // no point in trying to store commits for ram index
				return false;
			// no often than specified indexing interval
			return (LastIndexTime - PreviousIndexTime > context.Configuration.MinIndexingTimeIntervalToStoreCommitPoint ||
				// at least once for specified time interval
					LastIndexTime - LastCommitPointStoreTime > context.Configuration.MaxIndexCommitPointStoreTimeInterval);
		}

		private IndexingResult GetIndexingResult(object doc, AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, out float boost)
		{
			boost = 1;

			var boostedValue = doc as BoostedValue;
			if (boostedValue != null)
			{
				doc = boostedValue.Value;
				boost = boostedValue.Boost;
			}

			IndexingResult indexingResult;

			var docAsDynamicJsonObject = doc as DynamicJsonObject;

			// ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
			if (docAsDynamicJsonObject != null)
				indexingResult = ExtractIndexDataFromDocument(anonymousObjectToLuceneDocumentConverter, docAsDynamicJsonObject);
			else
				indexingResult = ExtractIndexDataFromDocument(anonymousObjectToLuceneDocumentConverter, doc);

			if (Math.Abs(boost - 1) > float.Epsilon)
			{
				foreach (var abstractField in indexingResult.Fields)
				{
					abstractField.OmitNorms = false;
				}
			}

			return indexingResult;
		}

		private class IndexingResult
		{
			public string NewDocId;
			public List<AbstractField> Fields;
			public bool ShouldSkip;
		}

		private IndexingResult ExtractIndexDataFromDocument(AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, DynamicJsonObject dynamicJsonObject)
		{
			var newDocIdAsObject = dynamicJsonObject.GetRootParentOrSelf().GetDocumentId();
			var newDocId = newDocIdAsObject is DynamicNullObject ? null : (string)newDocIdAsObject;
			List<AbstractField> abstractFields;

			try
			{
				abstractFields = anonymousObjectToLuceneDocumentConverter.Index(((IDynamicJsonObject)dynamicJsonObject).Inner, Field.Store.NO).ToList();
			}
			catch (InvalidShapeException e)
			{
				throw new InvalidSpatialShapException(e, newDocId);
			}

			return new IndexingResult
			{
				Fields = abstractFields,
				NewDocId = newDocId,
				ShouldSkip = false
			};
		}

		private readonly ConcurrentDictionary<Type, PropertyDescriptorCollection> propertyDescriptorCache = new ConcurrentDictionary<Type, PropertyDescriptorCollection>();

		private IndexingResult ExtractIndexDataFromDocument(AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, object doc)
		{
			PropertyDescriptorCollection properties;
			var newDocId = GetDocumentIdByReflection(doc, out properties);

			List<AbstractField> abstractFields;
			try
			{
				abstractFields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, Field.Store.NO).ToList();
			}
			catch (InvalidShapeException e)
			{
				throw new InvalidSpatialShapException(e, newDocId);
			}

			return new IndexingResult
			{
				Fields = abstractFields,
				NewDocId = newDocId,
				ShouldSkip = properties.Count > 1  // we always have at least __document_id
							&& abstractFields.Count == 0
			};
		}

		private string GetDocumentIdByReflection(object doc, out PropertyDescriptorCollection properties)
		{
			Type type = doc.GetType();
			properties = propertyDescriptorCache.GetOrAdd(type, TypeDescriptor.GetProperties);
			return properties.Find(Constants.DocumentIdFieldName, false).GetValue(doc) as string;
		}

		public override void Remove(string[] keys, WorkContext context)
		{
			Write((writer, analyzer, stats) =>
			{
				stats.Operation = IndexingWorkStats.Status.Ignore;
				logIndexing.Debug(() => string.Format("Deleting ({0}) from {1}", string.Join(", ", keys), indexId));
				var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(indexId))
					.Where(x => x != null)
					.ToList();

				keys.Apply(
					key =>
					InvokeOnIndexEntryDeletedOnAllBatchers(batchers, new Term(Constants.DocumentIdFieldName, key)));

				writer.DeleteDocuments(keys.Select(k => new Term(Constants.DocumentIdFieldName, k.ToLowerInvariant())).ToArray());
				batchers.ApplyAndIgnoreAllErrors(
					e =>
					{
						logIndexing.WarnException("Failed to dispose on index update trigger", e);
						context.AddError(indexId, null, e.Message, "Dispose Trigger");
					},
					batcher => batcher.Dispose());

				return new IndexedItemsInfo(GetLastEtagFromStats())
				{
					ChangedDocs = keys.Length,
					DeletedKeys = keys
				};
			});
		}

		/// <summary>
		/// For index recovery purposes
		/// </summary>
		internal void RemoveDirectlyFromIndex(string[] keys, Etag lastEtag)
		{
			Write((writer, analyzer, stats) =>
			{
				stats.Operation = IndexingWorkStats.Status.Ignore;

				writer.DeleteDocuments(keys.Select(k => new Term(Constants.DocumentIdFieldName, k.ToLowerInvariant())).ToArray());

				return new IndexedItemsInfo(lastEtag) // just commit, don't create commit point and add any infor about deleted keys
				{
					ChangedDocs = keys.Length,
					DisableCommitPoint = true
				};
			});
		}
	}
}
