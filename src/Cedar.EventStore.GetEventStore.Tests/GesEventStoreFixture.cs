namespace Cedar.EventStore
{
    using System.Diagnostics;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.ClientAPI.SystemData;
    using global::EventStore.Core;
    using Xunit.Abstractions;
    using ExpectedVersion = Cedar.EventStore.Streams.ExpectedVersion;
    using ReadDirection = Cedar.EventStore.Streams.ReadDirection;

    internal class GesEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public GesEventStoreFixture(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        public override async Task<IEventStore> GetEventStore()
        {
            var stopwatch = Stopwatch.StartNew();

            var node = await CreateClusterVNode();

            var connectionSettingsBuilder = ConnectionSettings
                .Create()
                .SetDefaultUserCredentials(new UserCredentials("admin", "changeit"))
                .KeepReconnecting();

            var gesEventStore = new GesEventStore(
                () => EmbeddedEventStoreConnection.Create(node, connectionSettingsBuilder));

            _testOutputHelper.WriteLine($"Fixture init {stopwatch.ElapsedMilliseconds}ms.");

            return new EventStoreWrapper(gesEventStore, node);
        }

        private static async Task<ClusterVNode> CreateClusterVNode()
        {
            var noEndpoint = new IPEndPoint(IPAddress.None, 0);

            ClusterVNode node = EmbeddedVNodeBuilder
                .AsSingleNode()
                .WithExternalTcpOn(noEndpoint)
                .WithInternalTcpOn(noEndpoint)
                .WithExternalHttpOn(noEndpoint)
                .WithInternalHttpOn(noEndpoint)
                .RunProjections(ProjectionsMode.All)
                .WithTfChunkSize(16000000)
                .RunInMemory();

            await node.StartAndWaitUntilInitialized();

            return node;
        }

        private class EventStoreWrapper : IEventStore
        {
            private readonly IEventStore _inner;
            private readonly ClusterVNode _node;

            public EventStoreWrapper(IEventStore inner, ClusterVNode node)
            {
                _inner = inner;
                _node = node;
            }

            public void Dispose()
            {
                _inner.Dispose();
                _node.StopNonblocking(true, true);
            }

            public Task AppendToStream(
                string streamId,
                int expectedVersion,
                NewStreamEvent[] events,
                CancellationToken cancellationToken = default(CancellationToken))
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
                string fromCheckpoint,
                int maxCount,
                ReadDirection direction = ReadDirection.Forward,
                CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.ReadAll(fromCheckpoint, maxCount, direction, cancellationToken);
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

            public Task<IStreamSubscription> SubscribeToStream(
                string streamId,
                int startPosition,
                StreamEventReceived streamEventReceived,
                SubscriptionDropped subscriptionDropped,
                string name,
                CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.SubscribeToStream(streamId,
                    startPosition,
                    streamEventReceived,
                    subscriptionDropped,
                    name,
                    cancellationToken);
            }

            public Task<IAllStreamSubscription> SubscribeToAll(
                string fromCheckpoint,
                StreamEventReceived streamEventReceived,
                SubscriptionDropped subscriptionDropped = null,
                string name = null,
                CancellationToken cancellationToken = default(CancellationToken))
            {
                return _inner.SubscribeToAll(fromCheckpoint, streamEventReceived, subscriptionDropped, name, cancellationToken);
            }

            public string StartCheckpoint => _inner.StartCheckpoint;

            public string EndCheckpoint => _inner.EndCheckpoint;
        }
    }
}