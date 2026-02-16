using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp;

public sealed class UpdateSnmpDatabaseEtlsMappingCommand : UpdateValueForDatabaseCommand
{
    public static string GetStorageKey(string databaseName)
    {
        return $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}/monitoring/snmp/etls/mapping";
    }
    
    private string _itemId;

    public List<string> Etls { get; set; }

    public UpdateSnmpDatabaseEtlsMappingCommand()
    {
        // for deserialization
    }
    
    public UpdateSnmpDatabaseEtlsMappingCommand(string databaseName, List<string> etls, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        Etls = etls;
    }
    
    public override string GetItemId()
    {
        return _itemId ??= GetStorageKey(DatabaseName);
    }
    
    public override void FillJson(DynamicJsonValue json)
    {
        if (Etls == null)
            return;

        json[nameof(Etls)] = new DynamicJsonArray(Etls);
    }

    protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject previousValue)
    {
        if (previousValue != null)
        {
            if (previousValue.Modifications == null)
                previousValue.Modifications = new DynamicJsonValue();

            AddEtlsIfNecessary(previousValue.Modifications, previousValue, Etls);

            if (previousValue.Modifications.Properties.Count == 0)
            {
                return new UpdatedValue(UpdatedValueActionType.Noop, value: null);
            }

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(previousValue, GetItemId()));
        }

        var djv = new DynamicJsonValue();
        AddEtlsIfNecessary(djv, null, Etls);

        return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(djv, GetItemId()));
    }
    
    private static void AddEtlsIfNecessary(DynamicJsonValue djv, BlittableJsonReaderObject previousValue, List<string> etls)
    {
        if (etls == null)
            return;
    
        var propertiesCount = previousValue?.Count ?? 0;
        foreach (var etl in etls)
        {
            if (previousValue == null || previousValue.TryGet(etl, out long _) == false)
                djv[etl] = propertiesCount + djv.Properties.Count + 1;
        }
    }
}
