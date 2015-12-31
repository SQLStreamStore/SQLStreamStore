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
    using Xunit;
    using Xunit.Abstractions;

    public class Exploratory
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public Exploratory(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task WHat_happens_when_a_stream_is_deleted()
        {
            IPEndPoint noEndpoint = new IPEndPoint(IPAddress.None, 0);

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

            var connectionSettingsBuilder = ConnectionSettings
                .Create()
                .SetDefaultUserCredentials(new UserCredentials("admin", "changeit"))
                .KeepReconnecting();

            using(var connection = EmbeddedEventStoreConnection.Create(node, connectionSettingsBuilder))
            {
                using(await connection.SubscribeToStreamAsync(
                    "stream-1",
                    true,
                    (subscription, @event) => { }))
                {
                    await connection.AppendToStreamAsync("stream-1",
                        global::EventStore.ClientAPI.ExpectedVersion.Any,
                        new EventData(Guid.NewGuid(), "event", true, Encoding.UTF8.GetBytes("{}"), null));

                    await connection.DeleteStreamAsync("stream-1", global::EventStore.ClientAPI.ExpectedVersion.Any);

                    await Task.Delay(1000);
                }
                int i = 0;
                using (await connection.SubscribeToAllAsync(
                    true,
                    (subscription, @event) =>
                    {
                        _testOutputHelper.WriteLine($"Event {i++}");
                        _testOutputHelper.WriteLine($" {@event.Event.EventType}");
                        _testOutputHelper.WriteLine($" {@event.Event.EventStreamId}");
                        _testOutputHelper.WriteLine($" {@event.Event.IsJson}");
                        _testOutputHelper.WriteLine($" {Encoding.UTF8.GetString(@event.Event.Data)}");
                    }))
                {
                    await connection.AppendToStreamAsync("stream-2",
                        global::EventStore.ClientAPI.ExpectedVersion.Any,
                        new EventData(Guid.NewGuid(), "myevent", true, Encoding.UTF8.GetBytes("{}"), null));

                    await connection.DeleteStreamAsync("stream-2", global::EventStore.ClientAPI.ExpectedVersion.Any);

                    await Task.Delay(1000);
                }
            }
        }
    }
}