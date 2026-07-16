using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public sealed class CompactSettings
    {
        public string DatabaseName { get; set; }

        public bool Documents { get; set; }

        public string[] Indexes { get; set; }

        public bool SkipOptimizeIndexes { get; set; }

        /// <summary>
        /// Optional path to a directory (can be on a different drive) that will host the temporary data of the compaction.
        /// Use when the drive holding the database does not have enough free space to compact in place.
        /// The compacted data is written there and copied back into the database directory afterwards, which is slower than compacting in place.
        /// The path must be absolute.
        /// </summary>
        public string TempPath { get; set; }

        /// <summary>
        /// Applies only when the compaction runs on a different path (see <see cref="TempPath"/>).
        /// When set, the original data is deleted before the compacted data is copied back, minimizing the free space
        /// required on the database drive. If the copy back fails, the database has to be restored manually
        /// from the preserved compacted data, so use only when the database drive cannot hold both copies.
        /// </summary>
        public bool DeleteOriginalBeforeCopyBack { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(Documents)] = Documents,
                [nameof(Indexes)] = Indexes,
                [nameof(SkipOptimizeIndexes)] = SkipOptimizeIndexes,
                [nameof(TempPath)] = TempPath,
                [nameof(DeleteOriginalBeforeCopyBack)] = DeleteOriginalBeforeCopyBack
            };
        }
    }
}
