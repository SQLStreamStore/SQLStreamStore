namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Net;
    using System.Threading.Tasks;
    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.Core;
    using global::EventStore.Core.Data;

    internal class GesEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        private static readonly IPEndPoint s_noEndpoint = new IPEndPoint(IPAddress.None, 0);

        public override async Task<IEventStore> GetEventStore()
        {
            var node = await CreateClusterVNode();
            var gesEventStore = new GesEventStore(_ => EmbeddedEventStoreConnection.Create(node));
            return new EventStoreWrapper(gesEventStore, node);
        }

        private static Task<ClusterVNode> CreateClusterVNode()
        {
            ClusterVNode node = EmbeddedVNodeBuilder
                .AsSingleNode()
                .WithExternalTcpOn(s_noEndpoint)
                .WithInternalTcpOn(s_noEndpoint)
                .WithExternalHttpOn(s_noEndpoint)
                .WithInternalHttpOn(s_noEndpoint)
                .RunProjections(ProjectionsMode.All)
                .RunInMemory();

            var tcs = new TaskCompletionSource<ClusterVNode>();

            node.NodeStatusChanged += (_, e) =>
            {
                if (e.NewVNodeState != VNodeState.Master)
                {
                    return;
                }
                tcs.SetResult(node);
            };
            node.Start();

            return tcs.Task;
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

            public Task AppendToStream(
                string storeId,
                string streamId,
                int expectedVersion,
                IEnumerable<NewStreamEvent> events)
            {
                return _inner.AppendToStream(storeId, streamId, expectedVersion, events);
            }

            public Task DeleteStream(string storeId, string streamId, int expectedVersion = ExpectedVersion.Any)
            {
                return _inner.DeleteStream(storeId, streamId, expectedVersion);
            }

            public Task<AllEventsPage> ReadAll(
                string storeId,
                string checkpoint,
                int maxCount,
                ReadDirection direction = ReadDirection.Forward)
            {
                return _inner.ReadAll(storeId, checkpoint, maxCount, direction);
            }

            public Task<StreamEventsPage> ReadStream(
                string storeId,
                string streamId,
                int start,
                int count,
                ReadDirection direction = ReadDirection.Forward)
            {
                return _inner.ReadStream(storeId, streamId, start, count, direction);
            }
        }
    }
}