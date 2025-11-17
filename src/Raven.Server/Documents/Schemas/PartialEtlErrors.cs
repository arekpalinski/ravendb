using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class PartialEtlErrors
{
    public static TableSchema Current => PartialEtlErrorsSchemaBase;

    public static TableSchema PartialEtlErrorsSchemaBase = new();

    public static readonly Slice ByCreatedAt;
    public static readonly Slice BySeverity;
    public static readonly Slice ByEtlTaskName;

    public const string PartialEtlErrorsTree = "PartialEtlErrors";

    public static class PartialEtlErrorsTable
    {
        public const int IdIndex = 0;
        public const int EtlTaskNameIndex = 1;
        public const int CreatedAtIndex = 2;
        public const int DocumentIdIndex = 3;
        public const int StepIndex = 4;
        public const int SeverityIndex = 5;
    }

    static PartialEtlErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByEtlTaskName", ByteStringType.Immutable, out ByEtlTaskName);
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
            Slice.From(ctx, "BySeverity", ByteStringType.Immutable, out BySeverity);
        }

        PartialEtlErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = PartialEtlErrorsTable.IdIndex,
            Count = 1
        });
        
        PartialEtlErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = PartialEtlErrorsTable.EtlTaskNameIndex,
            Name = ByEtlTaskName
        });

        PartialEtlErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = PartialEtlErrorsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });
        
        PartialEtlErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = PartialEtlErrorsTable.SeverityIndex,
            Name = BySeverity
        });
    }
}
