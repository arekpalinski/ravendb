using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Voron.Recovery
{
    public class RecoveredDatabaseCreator
    {
        private readonly string _recoveryDatabasePath;

        public RecoveredDatabaseCreator(string recoveryDatabasePath)
        {
            _recoveryDatabasePath = recoveryDatabasePath;
        }

        public DocumentDatabase CreateStorage()
        {
            // TODO arek -encrypted db 

            var serverConfiguration = RavenConfiguration.CreateForServer("recovery-server");

            serverConfiguration.Core.RunInMemory = true;

            serverConfiguration.Initialize();

            serverConfiguration.Core.RunInMemory = true;

            //RavenServer ravenServerBase = new RavenServer(serverConfiguration)
            //{
            //    WebUrl = "http://127.0.0.1:1111"
            //};

            //// ravenServerBase.Initialize();

            //var recoveryServer = new RecoveryServer();

            //var serverStore = new ServerStore(serverConfiguration, /* new RecoveryServer()*/ ravenServerBase);

            //serverStore.Initialize();

            ////serverStore.Cluster.ReadRawDatabaseRecord()

            //var dbConfiguration = RavenConfiguration.CreateForDatabase(serverConfiguration, "recovery-database");

            //dbConfiguration.Initialize();

            //var db = new DocumentDatabase("recovery-database", dbConfiguration, serverStore, x => { }); // serverStore.DatabasesLandlord.TryGetOrCreateResourceStore("recovery-database").Result;

            //db.Initialize();

            new DocumentsStorage(DatabaseStorageOptions.CreateForRecovery("recovered-db",), )

            return db;
        }
    }
}
