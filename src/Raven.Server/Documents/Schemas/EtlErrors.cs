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
    public static readonly Slice ByEtlTaskName;

    public const string EtlErrorsTree = "EtlErrors";

    public static class EtlErrorsTable
    {
        public const int IdIndex = 0;
        public const int EtlTaskNameIndex = 1;
        public const int CreatedAtIndex = 2;
        public const int AffectedDocumentsCountIndex = 3;
        public const int StepIndex = 4;
        public const int SeverityIndex = 5;
    }

    static EtlErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByEtlTaskName", ByteStringType.Immutable, out ByEtlTaskName);
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
            Slice.From(ctx, "BySeverity", ByteStringType.Immutable, out BySeverity);
        }

        EtlErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = EtlErrorsTable.IdIndex,
            Count = 1
        });
        
        EtlErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = EtlErrorsTable.EtlTaskNameIndex,
            Name = ByEtlTaskName
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
