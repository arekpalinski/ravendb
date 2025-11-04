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

    public const string PartialEtlErrorsTree = "PartialEtlErrors";

    public static class PartialEtlErrorsTable
    {
        public const int IdIndex = 0;
        public const int CreatedAtIndex = 1;
        public const int DocumentIdIndex = 2;
        public const int StepIndex = 3;
        public const int SeverityIndex = 4;
    }

    static PartialEtlErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
            Slice.From(ctx, "BySeverity", ByteStringType.Immutable, out BySeverity);
        }

        PartialEtlErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = PartialEtlErrorsTable.IdIndex,
            Count = 1
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
