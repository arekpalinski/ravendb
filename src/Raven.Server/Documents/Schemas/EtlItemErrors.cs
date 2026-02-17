using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class EtlItemErrors
{
    public static TableSchema Current => EtlItemErrorsSchemaBase;

    public static TableSchema EtlItemErrorsSchemaBase = new();

    public static readonly Slice ByCreatedAt;
    public static readonly Slice ByEtlProcessName;

    public const string EtlItemErrorsTree = "EtlItemErrors";

    public static class EtlItemErrorsTable
    {
        public const int IdIndex = 0;
        public const int EtlProcessNameIndex = 1;
        public const int CreatedAtIndex = 2;
        public const int DocumentIdIndex = 3;
        public const int StepIndex = 4;
        public const int ErrorIndex = 5;
        public const int AdditionalInfoIndex = 6;
    }

    static EtlItemErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByEtlProcessName", ByteStringType.Immutable, out ByEtlProcessName);
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
        }

        EtlItemErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = EtlItemErrorsTable.IdIndex,
            Count = 1
        });
        
        EtlItemErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = EtlItemErrorsTable.EtlProcessNameIndex,
            Name = ByEtlProcessName
        });

        EtlItemErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = EtlItemErrorsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });
    }
}
