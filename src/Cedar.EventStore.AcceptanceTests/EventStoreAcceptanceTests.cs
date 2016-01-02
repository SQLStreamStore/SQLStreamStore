namespace Cedar.EventStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;
    using Xunit.Abstractions;

    public partial class EventStoreAcceptanceTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public EventStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task When_dispose_and_read_then_should_throw()
        {
            using(var fixture = GetFixture())
            {
                var store = await fixture.GetEventStore();
                store.Dispose();

                Func<Task> act = () => store.ReadAll(store.StartCheckpoint, 10);

                act.ShouldThrow<ObjectDisposedException>();
            }
        }

        [Fact]
        public async Task Can_dispose_more_than_once()
        {
            using (var fixture = GetFixture())
            {
                var store = await fixture.GetEventStore();
                store.Dispose();

                Action act = store.Dispose;

                act.ShouldNotThrow();
            }
        }

        private static NewStreamEvent[] CreateNewStreamEvents(params int[] eventNumbers)
        {
            return eventNumbers
                .Select(eventNumber =>
                {
                    var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
                    return new NewStreamEvent(eventId, "type", "\"data\"", "\"metadata\"");
                })
                .ToArray();
        }

        private static StreamEvent ExpectedStreamEvent(
            string streamId,
            int eventNumber,
            int sequenceNumber,
            DateTime created)
        {
            var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
            return new StreamEvent(streamId, eventId, sequenceNumber, null, created, "type", "\"data\"", "\"metadata\"");
        }
    }
}