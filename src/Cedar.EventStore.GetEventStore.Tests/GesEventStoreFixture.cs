namespace Cedar.EventStore
{
    using System.Net;
    using System.Threading.Tasks;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.ClientAPI.SystemData;
    using global::EventStore.Core;

    public class GesEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        private readonly ClusterVNode _node;
        private readonly IEventStoreConnection _connection;

        public override int LargeSubscriptionCount => 10;

        public GesEventStoreFixture()
        {
            var none = new IPEndPoint(IPAddress.None, 0);

            _node = EmbeddedVNodeBuilder
                .AsSingleNode()
                .RunInMemory()
                .RunProjections(ProjectionsMode.All)
                .WithTfChunkSize(1024*1024)
                .WithInternalHttpOn(none)
                .WithExternalHttpOn(none)
                .WithInternalTcpOn(none)
                .WithExternalTcpOn(none);

            _connection = EmbeddedEventStoreConnection.Create(_node);
        }
        public override async Task<IEventStore> GetEventStore()
        {
            await _node.StartAndWaitUntilReady();

            await _connection.ConnectAsync();

            return new GesEventStore(_connection, new UserCredentials("admin", "changeit"));
        }

        public override void Dispose()
        {
            _node.Stop();
            _connection.Dispose();

            base.Dispose();
        }
    }
}