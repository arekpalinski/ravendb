using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class EtlErrors
{
    public static TableSchema Current => EtlErrorsSchemaBase;

    public static TableSchema EtlErrorsSchemaBase = new();

    public static readonly Slice ByCreatedAt;
    public static readonly Slice BySeverity;

    public const string EtlErrorsTree = "EtlErrors";

    public static class EtlErrorsTable
    {
        public const int IdIndex = 0;
        public const int CreatedAtIndex = 1;
        public const int AffectedDocumentsCountIndex = 2;
        public const int TypeIndex = 3;
        public const int SeverityIndex = 4;
    }

    static EtlErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
            Slice.From(ctx, "BySeverity", ByteStringType.Immutable, out BySeverity);
        }

        EtlErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = EtlErrorsTable.IdIndex,
            Count = 1
        });

        EtlErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = EtlErrorsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });
        
        EtlErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = EtlErrorsTable.SeverityIndex,
            Name = BySeverity
        });
    }
}
