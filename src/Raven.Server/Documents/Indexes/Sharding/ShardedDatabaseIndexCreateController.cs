﻿using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Sharding;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedDatabaseIndexCreateController : DatabaseIndexCreateController
{
    public ShardedDatabaseIndexCreateController([NotNull] ShardedDocumentDatabase database)
        : base(database)
    {
    }

    protected override void ValidateAutoIndex(IndexDefinitionBaseServerSide definition)
    {
        if (definition.DeploymentMode == IndexDeploymentMode.Rolling)
            throw new NotSupportedInShardingException("Rolling index deployment for a sharded database is currently not supported");

        base.ValidateAutoIndex(definition);
    }
}