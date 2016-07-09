namespace Cedar.EventStore
{
    using System;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.ClientAPI.SystemData;
    using global::EventStore.Core;
    using Shouldly;
    using Xunit;
    using Xunit.Abstractions;

    public class Exploratory : IDisposable
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private int _eventCounter;
        private readonly ClusterVNode _node;
        private readonly ConnectionSettingsBuilder _connectionSettingsBuilder;

        public Exploratory(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            IPEndPoint noEndpoint = new IPEndPoint(IPAddress.None, 0);

            _node = EmbeddedVNodeBuilder
                .AsSingleNode()
                .WithExternalTcpOn(noEndpoint)
                .WithInternalTcpOn(noEndpoint)
                .WithExternalHttpOn(noEndpoint)
                .WithInternalHttpOn(noEndpoint)
                .RunProjections(ProjectionsMode.All)
                .WithTfChunkSize(16000000)
                .RunInMemory();

            _connectionSettingsBuilder = ConnectionSettings
                .Create()
                .SetDefaultUserCredentials(new UserCredentials("admin", "changeit"))
                .KeepReconnecting();
        }

        [Fact]
        public async Task What_happens_when_a_stream_is_deleted()
        {
            await _node.StartAndWaitUntilInitialized();

            using(var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                using(await connection.SubscribeToStreamAsync("stream-1", true, PrintEvent))
                {
                    await connection.AppendToStreamAsync("stream-1",
                        ExpectedVersion.Any,
                        new EventData(Guid.NewGuid(), "event", true, Encoding.UTF8.GetBytes("{}"), null));

                    await connection.DeleteStreamAsync("stream-1", global::EventStore.ClientAPI.ExpectedVersion.Any);

                    await Task.Delay(1000);
                }

                using (await connection.SubscribeToAllAsync(true, PrintEvent))
                {
                    await connection.AppendToStreamAsync("stream-2",
                        ExpectedVersion.Any,
                        new EventData(Guid.NewGuid(), "myevent", true, Encoding.UTF8.GetBytes("{}"), null));

                    await connection.DeleteStreamAsync("stream-2", global::EventStore.ClientAPI.ExpectedVersion.Any);

                    await Task.Delay(1000);
                }
            }
        }

        [Fact]
        public async Task Can_append_event_with_duplicate_id_to_stream()
        {
            await _node.StartAndWaitUntilInitialized();

            using(var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "stream-1";
                var eventData = new EventData(Guid.NewGuid(), "type", false, null, null);

                await connection.AppendToStreamAsync(streamId, ExpectedVersion.NoStream, eventData);

                // I expected this to fail with a duplicate id exception of some sort, but it doesn't.
                // https://groups.google.com/d/msg/event-store/kq1K2cx3Ggo/9GZve-yzCAAJ
                await connection.AppendToStreamAsync(streamId, 0, eventData);

                var streamEventsSlice = await connection.ReadStreamEventsForwardAsync(streamId, StreamPosition.Start, 2, true);

                streamEventsSlice.Events.Length.ShouldBe(2);
            }
        }

        [Fact]
        public async Task Catchup_subscription()
        {
            await _node.StartAndWaitUntilInitialized();

            using(var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "stream-1";

                var eventData = new EventData(Guid.NewGuid(), "type-1", false, null, null);
                await connection.AppendToStreamAsync(streamId, ExpectedVersion.NoStream, eventData);

                var subscription = connection.SubscribeToStreamFrom(streamId,
                    null,
                    false,
                    (_, resolvedEvent) =>
                    {
                        _testOutputHelper.WriteLine($"Sub {resolvedEvent.OriginalEvent.EventNumber} {resolvedEvent.OriginalEvent.EventType}");
                    });

                var eventData2 = new EventData(Guid.NewGuid(), "type-2", false, null, null);
                await connection.AppendToStreamAsync(streamId, ExpectedVersion.Any, eventData2);

                await Task.Delay(2000);

                var streamEventsSlice = await connection.ReadStreamEventsForwardAsync(streamId, StreamPosition.Start, 10, true);
                foreach(var resolvedEvent in streamEventsSlice.Events)
                {
                    _testOutputHelper.WriteLine($"Slice {resolvedEvent.OriginalEvent.EventNumber} {resolvedEvent.OriginalEvent.EventType}");
                }
                subscription.Stop();
            }
        }

        [Fact]
        public async Task Read_all_events_forwards()
        {
            await _node.StartAndWaitUntilInitialized();

            using (var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "stream-1";
                var eventData = new EventData(Guid.NewGuid(), "type", false, null, null);
                await connection.AppendToStreamAsync(streamId, ExpectedVersion.NoStream, eventData);

                var allEventsSlice = await connection.ReadAllEventsForwardAsync(Position.Start, 200, true);

                foreach(var resolvedEvent in allEventsSlice.Events)
                {
                    _testOutputHelper.WriteLine(
                        $"{resolvedEvent.OriginalStreamId} {resolvedEvent.OriginalEventNumber} " +
                        $"{resolvedEvent.Event.EventType} {resolvedEvent.OriginalEvent.EventType}");

                }
            }
        }

        [Fact]
        public async Task Read_all_events_backwards()
        {
            await _node.StartAndWaitUntilInitialized();

            using (var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "stream-1";
                var eventData = new EventData(Guid.NewGuid(), "type", false, null, null);
                await connection.AppendToStreamAsync(streamId, ExpectedVersion.NoStream, eventData);

                var allEventsSlice = await connection.ReadAllEventsBackwardAsync(Position.End, 1000, true);

                _testOutputHelper.WriteLine($"FromPosition = {allEventsSlice.FromPosition}");
                _testOutputHelper.WriteLine($"NextPosition = {allEventsSlice.NextPosition}");

                foreach (var resolvedEvent in allEventsSlice.Events)
                {
                    _testOutputHelper.WriteLine(
                        $"{resolvedEvent.OriginalStreamId} {resolvedEvent.OriginalEventNumber} " +
                        $"{resolvedEvent.Event.EventType} {resolvedEvent.OriginalEvent.EventType}");
                }
            }
        }

        [Fact]
        public async Task Read_all_forwards()
        {
            await _node.StartAndWaitUntilInitialized();

            using(var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                var eventData = new EventData(Guid.NewGuid(), "type", false, null, null);
                await connection.AppendToStreamAsync("stream-1", ExpectedVersion.NoStream, eventData);

                var allEventsSlice = await connection.ReadAllEventsForwardAsync(Position.End, 1, true);

                _testOutputHelper.WriteLine($"FromPosition  {allEventsSlice.FromPosition}");
                _testOutputHelper.WriteLine($"Next Position {allEventsSlice.NextPosition}");
                _testOutputHelper.WriteLine($"Events Length {allEventsSlice.Events.Length}");
                //_testOutputHelper.WriteLine($"Events Length {allEventsSlice.Events[0].Event.EventType}");
            }
        }

        [Fact]
        public async Task What_happens_when_you_get_metadata_on_stream_that_does_not_exist()
        {
            await _node.StartAndWaitUntilInitialized();

            using (var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "does-not-exist";

                var metadataResult = await connection.GetStreamMetadataAsync(streamId);
                metadataResult.IsStreamDeleted.ShouldBe(false);
            }
        }

        [Fact]
        public async Task What_happens_when_you_set_metadata_on_stream_that_does_not_exist()
        {
            await _node.StartAndWaitUntilInitialized();

            using(var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "does-not-exist";
                var result = await connection
                    .SetStreamMetadataAsync(streamId, ExpectedVersion.NoStream, StreamMetadata.Create(maxCount: 2));

                var metadataResult = await connection.GetStreamMetadataAsync(streamId);
                metadataResult.IsStreamDeleted.ShouldBe(false);

                var events = await connection.ReadStreamEventsForwardAsync(streamId, StreamPosition.Start, 10, true);

                events.Status.ShouldBe(SliceReadStatus.StreamNotFound);
            }
        }

        [Fact]
        public async Task What_happens_when_you_delete_a_stream_that_has_metadata()
        {
            await _node.StartAndWaitUntilInitialized();

            using (var connection = EmbeddedEventStoreConnection.Create(_node, _connectionSettingsBuilder))
            {
                string streamId = "stream-1";
                var eventData = new EventData(Guid.NewGuid(), "type", false, null, null);

                await connection.AppendToStreamAsync(streamId, ExpectedVersion.NoStream, eventData);
                await connection
                    .SetStreamMetadataAsync(streamId, ExpectedVersion.NoStream, StreamMetadata.Create(maxCount: 2));

                await connection.DeleteStreamAsync(streamId, ExpectedVersion.Any, true);

                var metadata = await connection.GetStreamMetadataAsync(streamId);

                metadata.IsStreamDeleted.ShouldBe(true);
            }
        }

        public void Dispose()
        {
            _node.Stop();
        }

        private void PrintEvent(EventStoreSubscription eventStoreSubscription, ResolvedEvent resolvedEvent)
        {
            _testOutputHelper.WriteLine($"Event {_eventCounter++}");
            _testOutputHelper.WriteLine($" {eventStoreSubscription.StreamId}");
            _testOutputHelper.WriteLine($" {resolvedEvent.Event.EventType}");
            _testOutputHelper.WriteLine($" {resolvedEvent.Event.EventStreamId}");
            _testOutputHelper.WriteLine($" {resolvedEvent.Event.IsJson}");
            _testOutputHelper.WriteLine($" {Encoding.UTF8.GetString(resolvedEvent.Event.Data)}");
        }
    }
}