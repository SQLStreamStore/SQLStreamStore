namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        [Theory]
        [MemberData("GetReadStreamTheories")]
        public async Task Can_read_streams(ReadStreamTheory theory)
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamEvents(4, 5, 6));

                    var streamEventsPage =
                        await eventStore.ReadStream(theory.StreamId, theory.Start, theory.PageSize, theory.Direction);

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
                        options =>
                        {
                            options.Excluding(streamEvent => streamEvent.Checkpoint);
                            options.Excluding(streamEvent => streamEvent.Created);
                            return options;
                        });
                }
            }
        }

        public static IEnumerable<object[]> GetReadStreamTheories()
        {
            var theories = new[]
            {
                new ReadStreamTheory("stream-1", StreamPosition.Start, ReadDirection.Forward, 2, 
                    new StreamEventsPage("stream-1", PageReadStatus.Success, 0, 2, 2, ReadDirection.Forward, false,
                          ExpectedStreamEvent("stream-1", 1, 0, SystemClock.GetUtcNow().UtcDateTime),
                          ExpectedStreamEvent("stream-1", 2, 1, SystemClock.GetUtcNow().UtcDateTime)))
            };

            return theories.Select(t => new object[] { t });
        }

        public class ReadStreamTheory
        {
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
            {
                StreamId = streamId;
                Start = start;
                Direction = direction;
                PageSize = pageSize;
                ExpectedStreamEventsPage = expectedStreamEventsPage;
            }
        }
    }
}
