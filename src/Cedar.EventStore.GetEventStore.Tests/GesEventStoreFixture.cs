namespace Cedar.EventStore
{
    using System;
    using System.Net;
    using System.Threading.Tasks;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.Core;
    using global::EventStore.Core.Data;

    internal class GesEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        private static readonly IPEndPoint s_noEndpoint = new IPEndPoint(IPAddress.None, 0);
        private readonly Func<IEventStoreConnection> _createConnection;
        private readonly ClusterVNode _node;
        private readonly TaskCompletionSource<bool> _connected = new TaskCompletionSource<bool>();

        public GesEventStoreFixture()
        {
            _node = EmbeddedVNodeBuilder
                .AsSingleNode()
                .WithExternalTcpOn(s_noEndpoint)
                .WithInternalTcpOn(s_noEndpoint)
                .WithExternalHttpOn(s_noEndpoint)
                .WithInternalHttpOn(s_noEndpoint)
                .RunProjections(ProjectionsMode.All);

            _node.NodeStatusChanged += (_, e) =>
            {
                if (e.NewVNodeState != VNodeState.Master)
                {
                    return;
                }
                _connected.SetResult(true);
            };
            _createConnection = () => EmbeddedEventStoreConnection.Create(_node);

            _node.Start();
        }

        public override async Task<IEventStore> GetEventStore()
        {
            await _connected.Task;
            return new GesEventStore(_createConnection);
        }

        public override void Dispose()
        {
            _node.Stop();
        }
    }
}