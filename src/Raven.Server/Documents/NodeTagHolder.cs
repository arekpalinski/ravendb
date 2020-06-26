using Raven.Server.ServerWide;

namespace Raven.Server.Documents
{
    public abstract class NodeTagHolder
    {
        public abstract string NodeTag { get; }
    }

    public class ServerNodeTagHolder : NodeTagHolder
    {
        private readonly ServerStore _serverStore;

        public ServerNodeTagHolder(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        public override string NodeTag => _serverStore.NodeTag;
    }
}
