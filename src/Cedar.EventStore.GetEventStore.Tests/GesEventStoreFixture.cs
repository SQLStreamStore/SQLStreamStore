namespace Cedar.EventStore
{
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.ClientAPI.SystemData;
    using global::EventStore.Core;

    internal class GesEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override async Task<IEventStore> GetEventStore()
        {
            var node = await CreateClusterVNode();

            var connectionSettingsBuilder = ConnectionSettings
                .Create()
                .SetDefaultUserCredentials(new UserCredentials("admin", "changeit"))
                .KeepReconnecting();

            var gesEventStore = new GesEventStore(
                () => EmbeddedEventStoreConnection.Create(node, connectionSettingsBuilder));
            return new EventStoreWrapper(gesEventStore, node);
        }

        private static async Task<ClusterVNode> CreateClusterVNode()
        {
            IPEndPoint noEndpoint = new IPEndPoint(IPAddress.None, 0);

            ClusterVNode node = EmbeddedVNodeBuilder
                .AsSingleNode()
                .WithExternalTcpOn(noEndpoint)
                .WithInternalTcpOn(noEndpoint)
                .WithExternalHttpOn(noEndpoint)
                .WithInternalHttpOn(noEndpoint)
                .RunProjections(ProjectionsMode.All)
                .RunInMemory();

            await node.StartAndWaitUntilInitialized();

            return node;
        }

        private class EventStoreWrapper : IEventStore
        {
            private readonly GesEventStore _inner;
            private readonly ClusterVNode _node;

            public EventStoreWrapper(GesEventStore inner, ClusterVNode node)
            {
                _inner = inner;
                _node = node;
            }

            public void Dispose()
            {
                _inner.Dispose();
                _node.Stop();
            }

            public Task AppendToStream(string streamId, int expectedVersion, NewStreamEvent[] events, CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.AppendToStream(streamId, expectedVersion, events, cancellationToken);
            }

            public Task DeleteStream(
                string streamId,
                int expectedVersion = ExpectedVersion.Any,
                CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.DeleteStream(streamId, expectedVersion, cancellationToken);
            }

            public Task<AllEventsPage> ReadAll(
                Checkpoint checkpoint,
                int maxCount,
                ReadDirection direction = ReadDirection.Forward,
                CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.ReadAll(checkpoint, maxCount, direction, cancellationToken);
            }

            public Task<StreamEventsPage> ReadStream(
                string streamId,
                int start,
                int count,
                ReadDirection direction = ReadDirection.Forward,
                CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.ReadStream(streamId, start, count, direction, cancellationToken);
            }
        }
    }
}