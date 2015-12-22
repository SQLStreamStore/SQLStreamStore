namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Theory]
        [MemberData("GetReadStreamTheories")]
        public async Task Can_read_streams_forwards_and_backwards(ReadStreamTheory theory)
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

                    streamEventsPage.FromStreamVersion.ShouldBe(expectedStreamEventsPage.FromStreamVersion);
                    streamEventsPage.LastStreamVersion.ShouldBe(expectedStreamEventsPage.LastStreamVersion);
                    streamEventsPage.NextStreamVersion.ShouldBe(expectedStreamEventsPage.NextStreamVersion);
                    streamEventsPage.ReadDirection.ShouldBe(expectedStreamEventsPage.ReadDirection);
                    streamEventsPage.IsEndOfStream.ShouldBe(expectedStreamEventsPage.IsEndOfStream);
                    streamEventsPage.Status.ShouldBe(expectedStreamEventsPage.Status);
                    streamEventsPage.StreamId.ShouldBe(expectedStreamEventsPage.StreamId);
                    streamEventsPage.Events.Count.ShouldBe(expectedStreamEventsPage.Events.Count);

                    streamEventsPage.Events.ShouldBe(expectedEvents);
                   /*     options =>
                        {
                            options.Excluding(streamEvent => streamEvent.Checkpoint);
                            options.Excluding(streamEvent => streamEvent.Created);
                            return options;
                        });*/
                }
            }
        }

        [Theory]
        [InlineData(ReadDirection.Forward, 0, 10)]
        [InlineData(ReadDirection.Backward, StreamPosition.End, 10)]
        public async Task Empty_Streams_return_StreamNotFound(ReadDirection direction, int start, int pageSize)
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    var streamEventsPage =
                        await eventStore.ReadStream("stream-does-not-exist", start, pageSize, direction);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        [Theory]
        [InlineData(ReadDirection.Forward, 0, 10)]
        [InlineData(ReadDirection.Backward, StreamPosition.End, 10)]
        public async Task Deleted_Streams_return_StreamDeleted(ReadDirection direction, int start, int pageSize)
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream("stream-1", ExpectedVersion.Any);

                    var streamEventsPage =
                        await eventStore.ReadStream("stream-1", start, pageSize, direction);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.StreamDeleted);
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
                          ExpectedStreamEvent("stream-1", 2, 1, SystemClock.GetUtcNow().UtcDateTime))),

                new ReadStreamTheory("not-exist", 1, ReadDirection.Forward, 2, 
                    new StreamEventsPage("not-exist", PageReadStatus.StreamNotFound, 1, -1, -1, ReadDirection.Forward, true)),

                new ReadStreamTheory("stream-2", 1, ReadDirection.Forward, 2, 
                    new StreamEventsPage("stream-2", PageReadStatus.Success, 1, 3, 2, ReadDirection.Forward, true,
                          ExpectedStreamEvent("stream-2", 5, 1, SystemClock.GetUtcNow().UtcDateTime),
                          ExpectedStreamEvent("stream-2", 6, 2, SystemClock.GetUtcNow().UtcDateTime))),

                new ReadStreamTheory("stream-1", StreamPosition.End, ReadDirection.Backward, 2, 
                    new StreamEventsPage("stream-1", PageReadStatus.Success, -1, 0, 2, ReadDirection.Backward, false,
                          ExpectedStreamEvent("stream-1", 3, 2, SystemClock.GetUtcNow().UtcDateTime),
                          ExpectedStreamEvent("stream-1", 2, 1, SystemClock.GetUtcNow().UtcDateTime))),

                 new ReadStreamTheory("stream-1", StreamPosition.End, ReadDirection.Backward, 4, 
                    new StreamEventsPage("stream-1", PageReadStatus.Success, -1, -1, 2, ReadDirection.Backward, true,
                          ExpectedStreamEvent("stream-1", 3, 2, SystemClock.GetUtcNow().UtcDateTime),
                          ExpectedStreamEvent("stream-1", 2, 1, SystemClock.GetUtcNow().UtcDateTime),
                          ExpectedStreamEvent("stream-1", 1, 0, SystemClock.GetUtcNow().UtcDateTime)))
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
