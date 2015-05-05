namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        protected abstract EventStoreAcceptanceTestFixture GetFixture();

        private static NewStreamEvent[] EventId(params int[] eventNumbers)
        {
            return eventNumbers
                .Select(eventNumber =>
                {
                    var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
                    return new NewStreamEvent(eventId, new byte[] { 1, 2 }, new byte[] { 3, 4 });
                })
                .ToArray();
        }

        private static StreamEvent ExpectedStreamEvent(string streamId, int eventNumber, int sequenceNumber)
        {
            var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
            return new StreamEvent(streamId, eventId, sequenceNumber, null, new byte[] { 1, 2 }, new byte[] { 3, 4 });
        }

        private static async Task InitializeEventStore(IEventStore eventStore)
        {
            await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, EventId(1, 2, 3));
            await eventStore.AppendToStream("stream-2", ExpectedVersion.NoStream, EventId(4, 5, 6));
        }

        public static IEnumerable<object[]> GetReadStreamTheories()
        {
            var theories = new[]
            {
                new ReadStreamTheory("stream-1", StreamPosition.Start, ReadDirection.Forward, 2, 
                    new StreamEventsPage("stream-1", PageReadStatus.Success, 0, 2, 2, ReadDirection.Forward, false,
                          ExpectedStreamEvent("stream-1", 1, 0), ExpectedStreamEvent("stream-1", 2, 1)))
            };

            return theories.Select(t => new object[] { t });
        }

        [Theory]
        [MemberData("GetReadStreamTheories")]
        public async Task Can_read_streams(ReadStreamTheory theory)
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    await InitializeEventStore(eventStore);

                    var streamEventsPage = await eventStore.ReadStream(theory.StreamId, theory.Start, 2, theory.Direction);

                    var expectedStreamEventsPage = theory.ExpectedStreamEventsPage;
                    var expectedEvents = theory.ExpectedStreamEventsPage.Events;

                    streamEventsPage.FromSequenceNumber.Should().Be(expectedStreamEventsPage.FromSequenceNumber);
                    streamEventsPage.IsEndOfStream.Should().Be(expectedStreamEventsPage.IsEndOfStream);
                    streamEventsPage.LastSequenceNumber.Should().Be(expectedStreamEventsPage.LastSequenceNumber);
                    streamEventsPage.NextSequenceNumber.Should().Be(expectedStreamEventsPage.NextSequenceNumber);
                    streamEventsPage.ReadDirection.Should().Be(expectedStreamEventsPage.ReadDirection);
                    streamEventsPage.Status.Should().Be(expectedStreamEventsPage.Status);
                    streamEventsPage.StreamId.Should().Be(expectedStreamEventsPage.StreamId);
                    streamEventsPage.Events.Count.Should().Be(expectedStreamEventsPage.Events.Count);

                    streamEventsPage.Events.ShouldAllBeEquivalentTo(
                        expectedEvents,
                        options => options.Excluding(@event => @event.Checkpoint));
                }
            }
        }

        public class ReadStreamTheory
        {
            public readonly string StoreId;
            public readonly string StreamId;
            public readonly int Start;
            public readonly ReadDirection Direction;
            public readonly int PageSize;
            public readonly StreamEventsPage ExpectedStreamEventsPage;

            public ReadStreamTheory(
               string streamId,
               int start,
               ReadDirection direction,
               int pageSize,
               StreamEventsPage expectedStreamEventsPage)
                : this(DefaultStore.StoreId, streamId, start, direction, pageSize, expectedStreamEventsPage)
            {}

            public ReadStreamTheory(
                string storeId,
                string streamId,
                int start,
                ReadDirection direction,
                int pageSize,
                StreamEventsPage expectedStreamEventsPage)
            {
                StoreId = storeId;
                StreamId = streamId;
                Start = start;
                Direction = direction;
                PageSize = pageSize;
                ExpectedStreamEventsPage = expectedStreamEventsPage;
            }
        }

        [Fact]
        public async Task Can_delete_a_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var events = new[]
                    {
                        new NewStreamEvent(Guid.NewGuid(),
                            new byte[0],
                            new byte[0]),
                        new NewStreamEvent(Guid.NewGuid(), new byte[0])
                    };

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, events);
                    await eventStore.DeleteStream(streamId);

                    var streamEventsPage =
                        await eventStore.ReadStream(streamId, StreamPosition.End, 10, ReadDirection.Backward);

                    streamEventsPage.Status.Should().Be(PageReadStatus.StreamDeleted);
                }
            }
        }
    }
}
