using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class EtlProcessErrors
{
    public static TableSchema Current => EtlProcessErrorsSchemaBase;

    public static TableSchema EtlProcessErrorsSchemaBase = new();

    public static readonly Slice ByCreatedAt;
    public static readonly Slice ByEtlProcessName;

    public const string EtlProcessErrorsTree = "EtlProcessErrors";

    public static class EtlProcessErrorsTable
    {
        public const int IdIndex = 0;
        public const int EtlProcessNameIndex = 1;
        public const int CreatedAtIndex = 2;
        public const int AffectedDocumentsCountIndex = 3;
        public const int StepIndex = 4;
        public const int ErrorIndex = 5;
        public const int AdditionalInfoIndex = 6;
    }

    static EtlProcessErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByEtlProcessName", ByteStringType.Immutable, out ByEtlProcessName);
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
        }

        EtlProcessErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = EtlProcessErrorsTable.IdIndex,
            Count = 1
        });
        
        EtlProcessErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = EtlProcessErrorsTable.EtlProcessNameIndex,
            Name = ByEtlProcessName
        });

        EtlProcessErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = EtlProcessErrorsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });
    }
}
