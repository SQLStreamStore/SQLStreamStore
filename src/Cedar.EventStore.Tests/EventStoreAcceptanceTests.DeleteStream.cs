namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
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
                        new NewStreamEvent(Guid.NewGuid(), "data", "meta"),
                        new NewStreamEvent(Guid.NewGuid(), "data", "meta")
                    };

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, events);
                    await eventStore.DeleteStream(streamId);

                    var streamEventsPage =
                        await eventStore.ReadStream(streamId, StreamPosition.Start, 10);

                    streamEventsPage.Status.Should().Be(PageReadStatus.StreamDeleted);
                }
            }
        }

        [Fact]
        public async Task When_delete_a_stream_and_append_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1));
                    await eventStore.DeleteStream(streamId);

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(2))
                        .ShouldThrow<StreamDeletedException>();
                }
            }
        }
    }

    internal static class TaskExtensions
    {
        internal static async Task ShouldThrow<T>(this Task task)
        {
            try
            {
                await task;
            }
            catch(Exception ex)
            {
                ex.Should().BeOfType<T>();
            }
        }
    }
}